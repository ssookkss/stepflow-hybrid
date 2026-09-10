# Stepflow Hybrid Engine — 시퀀스 다이어그램

이 문서는 `main.go`(REST API 라우팅)와 `engine/hybrid.go`(하이브리드 분류·워커 풀·K3s 디스패처)의
전체 실행 흐름을 시퀀스 다이어그램으로 정리한 것이다. CLAUDE.md에 정의된 동시성 안전 규칙과
하이브리드 아키텍처 규칙(Lightweight/Heavy 분기)이 코드상 어떤 순서로 실행되는지 참고하는 용도로 사용한다.

## 1. 서버 시작 및 종료 (Graceful Shutdown)

`main()`이 상시 워커 풀을 기동하고, 종료 시그널 수신 시 HTTP 서버와 워커 풀을 순서대로 정리하는 흐름이다.

```mermaid
sequenceDiagram
    autonumber
    participant OS as OS (SIGINT/SIGTERM)
    participant Main as main()
    participant Engine as HybridEngine
    participant Pool as 상시 워커 풀 (goroutine N개)
    participant HTTP as http.Server

    Main->>Engine: NewHybridEngine(workerCount)
    Engine->>Pool: go lightweightWorker() × workerCount
    Note over Pool: jobQueue(버퍼 workerCount*4)를 대기하며 상시 유지
    Main->>HTTP: go server.ListenAndServe()
    Main->>Main: waitForShutdownSignal() (블로킹)

    OS-->>Main: SIGINT / SIGTERM 수신
    Main->>HTTP: server.Shutdown(ctx, 15s 타임아웃)
    HTTP-->>Main: 진행 중 요청 완료 후 종료
    Main->>Engine: hybridEngine.Shutdown()
    Engine->>Pool: close(jobQueue)
    Pool-->>Engine: 각 워커가 잔여 job 처리 후 루프 종료 (workerWG.Wait())
    Engine-->>Main: 종료 완료
```

## 2. 요청 접수 → 하이브리드 분류 (공통 흐름)

모든 요청은 아래 공통 경로를 거친 뒤 Lightweight/Heavy로 분기한다. 이후 두 분기의 상세 흐름은
3번, 4번 다이어그램을 참고한다.

```mermaid
sequenceDiagram
    autonumber
    actor Client as 웹 저작도구
    participant Bridge as main.go (HandleWorkflowExecute)
    participant Engine as HybridEngine.Dispatch

    Client->>Bridge: POST /api/workflow/execute (JSON: workflow_name, tasks[])
    Bridge->>Bridge: r.Method 검증
    alt Method != POST
        Bridge-->>Client: 405 Method Not Allowed {error}
    else POST
        Bridge->>Bridge: JSON 디코딩 (DisallowUnknownFields)
        alt 파싱 실패 또는 tasks 비어있음
            Bridge-->>Client: 400 Bad Request {error}
        else 요청 유효
            Bridge->>Engine: Dispatch(r.Context(), req)
            Engine->>Engine: newJobID() — UUID v4 발급 (crypto/rand)
            Engine->>Engine: ClassifyWorkflow(req.Tasks)
            Note right of Engine: tasks > 3개 이거나 action == "analyze" 포함 → Heavy<br/>그 외 → Lightweight
            alt mode == heavy
                Engine->>Engine: dispatchHeavy(ctx, jobID, req) → 3번 다이어그램
            else mode == lightweight
                Engine->>Engine: dispatchLightweight(ctx, jobID, req) → 4번 다이어그램
            end
            Engine-->>Bridge: ExecutionResult{job_id, mode, status, output_dir, ...}
            Bridge-->>Client: 200 OK (JSON) 또는 500 {error}
        end
    end
```

## 3. Heavy 모드 — K3s Pod-per-workflow

TaskNode 4개 이상이거나 action에 `analyze`가 포함된 경우, `client-go`로 K3s에 전용 Pod를 생성한다.

```mermaid
sequenceDiagram
    autonumber
    participant Engine as HybridEngine
    participant Storage as 공유 저장소 (SHARED_STORAGE_DIR)
    participant K8sClient as client-go Clientset
    participant K3s as K3s API 서버

    Engine->>Storage: outputDirForJob(jobID)
    Storage-->>Engine: registry/outputs/{job_id}/ 생성 완료

    Engine->>Engine: getK8sClientset() (sync.Once 지연 초기화)
    alt 최초 호출
        Engine->>Engine: buildKubernetesConfig()
        Note right of Engine: rest.InClusterConfig() 우선 시도,<br/>실패 시 ~/.kube/config(KUBECONFIG)로 폴백
        Engine->>K8sClient: kubernetes.NewForConfig(cfg)
    end

    Engine->>Engine: buildHeavyPodSpec(podName, jobID, workflowName)
    Note right of Engine: Pod 이름: workflow-heavy-{job_id}<br/>컨테이너 command: stepflow run --job-id {job_id} --workflow-name {workflow_name}<br/>컨테이너 env: PYTHONDONTWRITEBYTECODE=1, STEPFLOW_JOB_ID={job_id}<br/>HostPath 볼륨: /mnt/aqua_blue/romy_dev

    Engine->>K8sClient: Pods("default").Create(ctx, pod)
    K8sClient->>K3s: Pod 생성 요청
    alt 생성 성공
        K3s-->>K8sClient: 생성된 Pod 반환
        K8sClient-->>Engine: created.Name
        Engine-->>Engine: ExecutionResult{job_id, mode: heavy, status: submitted, output_dir, pod_name}
    else 생성 실패 (AlreadyExists 등)
        K3s-->>K8sClient: 오류 응답
        K8sClient-->>Engine: error
        Engine-->>Engine: fmt.Errorf("K3s Pod 생성 실패: %w", err)
    end
```

## 4. Lightweight 모드 — 상시 워커 풀 + `os/exec`

TaskNode 3개 이하이며 action에 `analyze`가 없는 경우, 상시 워커 풀 큐에 작업을 넣고 로컬에서
`stepflow run`을 실행한다.

```mermaid
sequenceDiagram
    autonumber
    participant Engine as HybridEngine
    participant Queue as jobQueue
    participant Worker as Worker
    participant FS as 세션 디렉토리
    participant Storage as 공유 저장소
    participant CLI as stepflow CLI

    Note over FS: os.MkdirTemp로 생성되는 요청 전용 임시 디렉토리
    Note over Storage: registry/outputs/{job_id}/

    Engine->>Queue: lightweightJob 전송
    Note right of Engine: ctx.Done() 시 큐잉/대기 즉시 취소
    Note right of Engine: (goroutine 누수 방지)
    Queue->>Worker: job 수신
    Worker->>Worker: runLightweight(ctx, jobID, req)

    Worker->>FS: 세션 디렉토리 생성
    Note right of FS: os.MkdirTemp("", "stepflow-session-{job_id}-")

    Worker->>Storage: 출력 디렉토리 생성
    Note right of Storage: outputDirForJob(jobID)

    Worker->>FS: workflow.yaml 생성
    Note right of FS: component = normalizeComponentPath(action)
    Note right of FS: input = params(JSON)
    Note right of FS: output = { $step: 마지막 스텝 id }

    Worker->>FS: stepflow-config.yml 생성
    Note right of FS: plugins/routes는 배포 환경 값 필요
    Note right of FS: (examples/stepflow-config.example.yml 참고)

    Worker->>CLI: stepflow run 실행
    Note right of CLI: --flow workflow.yaml --config stepflow-config.yml
    Note right of CLI: --output result.json --input-json "{}"
    Note right of CLI: env: PYTHONDONTWRITEBYTECODE=1
    Note right of CLI: 타임아웃: STEPFLOW_RUN_TIMEOUT_SECONDS(기본 300s)

    alt 실행 성공
        CLI-->>Storage: result.json 기록
        CLI-->>Worker: exit code 0
        Worker->>FS: 세션 디렉토리 삭제 (defer)
        Worker-->>Queue: 결과 전달
        Note right of Worker: ExecutionResult - job_id, mode=lightweight
        Note right of Worker: status=completed, output_dir
    else 실행 실패 / 타임아웃
        CLI-->>Worker: exit code != 0 또는 DeadlineExceeded
        Worker->>FS: 세션 디렉토리 삭제 (defer)
        Worker-->>Queue: 에러 전달
        Note right of Worker: error (fmt.Errorf로 래핑)
    end

    Queue-->>Engine: resultCh 수신 → 결과/에러 반환
```

## 참고: 소스 위치

| 구성 요소 | 파일 |
|---|---|
| REST API 라우팅, 요청 검증, graceful shutdown | `main.go` |
| 하이브리드 분류, 워커 풀, 세션 격리, K3s 디스패처 | `engine/hybrid.go` |
| plugins/routes 실제 설정 예시 | `examples/stepflow-config.example.yml` |
