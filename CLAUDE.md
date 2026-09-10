# CLAUDE.md - Stepflow Hybrid Engine & Bridge Project

## 프로젝트 개요
- **목적**: 웹 저작도구의 JSON 페이로드를 REST API로 수신하고, 공통 저장소(`SHARED_STORAGE_DIR`)와 연동하여 Stepflow 규격으로 변환 및 실행을 제어하는 Go 기반 동시성 안전 하이브리드 엔진.
- **타겟 환경**: CachyOS (Linux), Go 1.22+, 로컬 K3s 클러스터, 공유 파일 시스템.

## 동시성 및 안전성 핵심 규칙 (Concurrency & Safety Rules)
1. **요청 세션 완전 격리 (UUID/TempDir)**:
   - 모든 수신 요청은 고유 UUID(`job_id`)를 발급받고, `os.MkdirTemp`를 이용해 독립된 임시 디렉토리에서 `workflow.yaml`과 `stepflow-config.yml`을 생성하여 파일/포트 충돌을 원천 차단합니다.
2. **공유 저장소 및 출력 격리**:
   - 입력 데이터와 소스는 공통 저장소(`/home/romy/dev/work/suredatalab/stepflow-hybrid/registry`)에서 읽기 전용으로 참조합니다.
   - 모든 작업 결과물 및 출력 파일은 반드시 고유 디렉토리(`/home/romy/dev/work/suredatalab/stepflow-hybrid/registry/outputs/{job_id}/`)에 기록합니다.
   - 파이썬 바이트코드 충돌 방지를 위해 `PYTHONDONTWRITEBYTECODE=1` 환경 변수를 강제합니다.
3. **K3s Pod 이름 유일성**:
   - 쿠버네티스 Pod 생성 시 `workflow-heavy-{job_id}` 형식을 사용하여 이름 충돌(`AlreadyExists`)을 방지합니다.

## 하이브리드 아키텍처 규칙
1. **Lightweight (상시 워커 풀 + 로컬 실행)**:
   - 조건: TaskNode 수가 3개 이하이며, Action이 "analyze"가 아닌 경우.
   - 동작: Go 채널 워커 풀에서 처리, UUID 기반 격리 임시 폴더에서 `os/exec`로 로컬 `stepflow run` 실행.
2. **Heavy (Kubernetes Pod-per-workflow)**:
   - 조건: TaskNode가 4개 이상이거나, Action이 "analyze"인 경우.
   - 동작: `client-go`를 사용하여 K3s 클러스터에 공유 볼륨(`HostPath`)이 마운트된 유일한 Pod 생성.

## 디렉토리 구조 및 빌드
- `main.go`: REST API 서버 (`POST /api/workflow/execute`)
- `engine/hybrid.go`: 동시성 안전 분류, UUID 세션 격리, 워커 풀, K3s 공유 볼륨 디스패처
- **명령어**: `go build -o bin/bridge main.go` / `go run main.go`

## 코딩 스타일
* 주석은 상세히 작성
* 복잡한 비즈니스 로직이나 이유(WHY)를 중심으로 설명 주석 필수 작성
* 주석은 항상 한국어로 작성
* Go 표준 포맷팅 도구인 gofmt 스타일을 엄격히 준수
* 에러 발생 시 단순 문자열 반환을 지양하고, fmt.Errorf("컨텍스트: %w", err) 구조를 통해 원인 추적이 가능하도록 에러 래핑 적용
* REST API 라우팅(main.go)과 하이브리드 비즈니스 로직·디스패처(engine/hybrid.go)의 책임을 철저히 분리
