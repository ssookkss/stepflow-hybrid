// Package engine은 Stepflow 하이브리드 엔진의 핵심 비즈니스 로직을 담당한다.
// REST API 라우팅(main.go)과는 철저히 책임을 분리하여, 이 패키지는 오직
// "요청 격리 → 하이브리드 분류(Lightweight/Heavy) → 실행 디스패치"만을 담당한다.
package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// ---------------------------------------------------------------------------
// 상수 및 기본 설정값
// ---------------------------------------------------------------------------

const (
	// ModeLightweight: 상시 워커 풀 + 로컬 os/exec 실행 모드.
	ModeLightweight = "lightweight"
	// ModeHeavy: K3s Pod-per-workflow 실행 모드.
	ModeHeavy = "heavy"

	// LightweightMaxTasks: 이 값 이하의 TaskNode 개수만 Lightweight 모드 후보가 된다.
	LightweightMaxTasks = 3

	// AnalyzeAction: 이 액션이 포함되면 태스크 개수와 무관하게 무조건 Heavy 모드로 강제 전환된다.
	// (analyze는 CPU/메모리 사용량이 크고 실행 시간이 길어 상시 워커 풀을 오래 점유하면
	//  다른 경량 요청의 처리 지연을 유발하기 때문)
	AnalyzeAction = "analyze"

	// DefaultSharedStorageDir: SHARED_STORAGE_DIR 환경변수가 설정되지 않았을 때 사용하는 기본 공유 저장소 경로.
	DefaultSharedStorageDir = "/home/romy/dev/work/suredatalab/stepflow-hybrid/registry"

	// HeavyHostPathMount: Heavy 모드 Pod에 마운트할 공유 볼륨의 호스트 경로.
	// (프로젝트 루트가 /mnt/aqua_blue/romy_dev 실경로와 /home/romy/... 경로로 이중 접근 가능한
	//  환경 특성을 반영하여, K3s 노드 입장에서는 실경로인 /mnt/aqua_blue/romy_dev를 그대로 마운트한다)
	HeavyHostPathMount = "/mnt/aqua_blue/romy_dev"

	// DefaultHeavyWorkerImage: Heavy 모드 Pod에서 사용할 기본 컨테이너 이미지.
	// STEPFLOW_WORKER_IMAGE 환경변수로 오버라이드 가능하다.
	DefaultHeavyWorkerImage = "stepflow-worker:latest"

	// PythonDontWriteBytecodeEnv: 파이썬 바이트코드(.pyc) 캐시 충돌 방지를 위해 강제하는 환경 변수.
	PythonDontWriteBytecodeEnv = "PYTHONDONTWRITEBYTECODE=1"
)

// ---------------------------------------------------------------------------
// 요청/결과 데이터 모델
// ---------------------------------------------------------------------------

// TaskNode는 웹 저작도구가 전달하는 워크플로우 JSON 페이로드의 개별 작업 단위를 표현한다.
type TaskNode struct {
	ID     string                 `json:"id"`
	Action string                 `json:"action"`
	Params map[string]interface{} `json:"params,omitempty"`
}

// WorkflowRequest는 POST /api/workflow/execute 로 수신되는 JSON 페이로드 전체를 표현한다.
type WorkflowRequest struct {
	WorkflowName string     `json:"workflow_name"`
	Tasks        []TaskNode `json:"tasks"`
}

// ExecutionResult는 Lightweight/Heavy 실행 결과를 호출자(main.go)에게 반환하기 위한 공통 응답 구조체다.
type ExecutionResult struct {
	JobID     string `json:"job_id"`
	Mode      string `json:"mode"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	OutputDir string `json:"output_dir,omitempty"`
	PodName   string `json:"pod_name,omitempty"`
}

// ---------------------------------------------------------------------------
// 경로 정규화 및 안전 유틸리티
// ---------------------------------------------------------------------------

// GetSharedStorageDir는 SHARED_STORAGE_DIR 환경 변수를 조회하고,
// 설정되어 있지 않으면 CLAUDE.md에 명시된 기본 공유 저장소 경로를 반환한다.
func GetSharedStorageDir() string {
	if dir := os.Getenv("SHARED_STORAGE_DIR"); dir != "" {
		return dir
	}
	return DefaultSharedStorageDir
}

// ResolvePath는 공유 저장소(SHARED_STORAGE_DIR) 기준의 상대 경로를 절대 경로로 정규화한다.
// "../" 등을 이용한 경로 이탈(Path Traversal) 시도를 원천 차단하기 위해, 정규화 결과가
// 반드시 공유 저장소 하위 경로인지를 검증한 뒤에만 경로를 반환한다.
func ResolvePath(relativePath string) (string, error) {
	baseDir, err := filepath.Abs(GetSharedStorageDir())
	if err != nil {
		return "", fmt.Errorf("공유 저장소 절대 경로 변환 실패: %w", err)
	}

	joined := filepath.Join(baseDir, relativePath)

	// filepath.Join이 "../" 등을 정리(clean)해 주지만, 그 결과가 baseDir 바깥으로
	// 빠져나갔을 가능성을 반드시 재검증해야 한다.
	if joined != baseDir && !strings.HasPrefix(joined, baseDir+string(os.PathSeparator)) {
		return "", fmt.Errorf("허용되지 않은 공유 저장소 경로 접근 시도: %q", relativePath)
	}
	return joined, nil
}

// outputDirForJob은 job_id별로 격리된 출력 디렉토리
// (registry/outputs/{job_id}/)를 생성하고 그 절대 경로를 반환한다.
// 모든 작업 결과물은 이 디렉토리에만 기록되어야 하며, 다른 job_id의 산출물과 절대
// 섞이지 않도록 보장한다.
func outputDirForJob(jobID string) (string, error) {
	outputDir, err := ResolvePath(filepath.Join("outputs", jobID))
	if err != nil {
		return "", fmt.Errorf("출력 디렉토리 경로 계산 실패 (job_id=%s): %w", jobID, err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("출력 디렉토리 생성 실패 (job_id=%s): %w", jobID, err)
	}
	return outputDir, nil
}

// newJobID는 요청마다 유일한 job_id(UUID v4)를 생성한다.
// 외부 UUID 라이브러리 의존성을 추가하지 않기 위해 crypto/rand로 직접 RFC 4122 v4 규격을 구현한다.
func newJobID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand 실패는 시스템 엔트로피 고갈 등 극히 이례적인 상황이므로,
		// 서비스 중단을 막기 위해 나노초 타임스탬프 기반 폴백으로 최소한의 유일성만 보장한다.
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // 버전 4 비트 설정
	buf[8] = (buf[8] & 0x3f) | 0x80 // RFC 4122 variant 비트 설정
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// ---------------------------------------------------------------------------
// 하이브리드 분류
// ---------------------------------------------------------------------------

// ClassifyWorkflow는 TaskNode 목록을 분석하여 Lightweight/Heavy 실행 모드를 결정한다.
// 규칙(CLAUDE.md 하이브리드 아키텍처 규칙 기준):
//   - TaskNode 4개 이상 → Heavy
//   - Action에 "analyze"가 하나라도 포함 → Heavy (개수 무관)
//   - 그 외(3개 이하 & analyze 미포함) → Lightweight
func ClassifyWorkflow(tasks []TaskNode) string {
	if len(tasks) > LightweightMaxTasks {
		return ModeHeavy
	}
	for _, task := range tasks {
		if strings.EqualFold(task.Action, AnalyzeAction) {
			return ModeHeavy
		}
	}
	return ModeLightweight
}

// ---------------------------------------------------------------------------
// HybridEngine: 동시성 안전 디스패처
// ---------------------------------------------------------------------------

// HybridEngine은 상시 워커 풀(Lightweight)과 K3s 클라이언트(Heavy)를 함께 관리하는
// 하이브리드 실행 엔진이다. main.go는 이 구조체 하나만 알면 되며, 내부 구현(워커 풀,
// client-go 초기화 등)에는 관여하지 않는다.
type HybridEngine struct {
	jobQueue chan lightweightJob
	workerWG sync.WaitGroup

	// K3s clientset은 최초 Heavy 요청 시점에 지연 초기화(sync.Once)한다.
	// 로컬 개발 환경 등 K3s 클러스터가 없는 상태에서도 Lightweight 요청은
	// 정상 동작해야 하기 때문이다.
	k8sOnce      sync.Once
	k8sClientset kubernetes.Interface
	k8sInitErr   error
}

type lightweightJob struct {
	ctx      context.Context
	jobID    string
	request  WorkflowRequest
	resultCh chan<- lightweightOutcome
}

type lightweightOutcome struct {
	result *ExecutionResult
	err    error
}

// NewHybridEngine은 workerCount개의 고정 워커 고루틴으로 구성된 상시 워커 풀을 기동한다.
// 워커 풀은 프로세스 생명주기 동안 유지되며, Lightweight 요청은 os/exec 호출 없이도
// 큐잉만으로 즉시 접수된다(요청 폭주 시에도 무한정 고루틴이 생성되지 않도록 방지).
func NewHybridEngine(workerCount int) *HybridEngine {
	if workerCount <= 0 {
		workerCount = 4
	}

	engine := &HybridEngine{
		jobQueue: make(chan lightweightJob, workerCount*4),
	}

	engine.workerWG.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go engine.lightweightWorker()
	}
	return engine
}

// Shutdown은 워커 풀을 정상 종료한다. main.go의 graceful shutdown 경로에서 호출한다.
func (e *HybridEngine) Shutdown() {
	close(e.jobQueue)
	e.workerWG.Wait()
}

// lightweightWorker는 상시 워커 풀의 워커 루프다. jobQueue가 닫히면 자연스럽게 종료된다.
func (e *HybridEngine) lightweightWorker() {
	defer e.workerWG.Done()
	for job := range e.jobQueue {
		result, err := runLightweight(job.ctx, job.jobID, job.request)
		job.resultCh <- lightweightOutcome{result: result, err: err}
	}
}

// Dispatch는 요청마다 고유 job_id를 발급한 뒤, ClassifyWorkflow 결과에 따라
// Lightweight 워커 풀 또는 Heavy K3s Pod 생성 경로로 작업을 위임한다.
func (e *HybridEngine) Dispatch(ctx context.Context, req WorkflowRequest) (*ExecutionResult, error) {
	jobID := newJobID()
	mode := ClassifyWorkflow(req.Tasks)

	if mode == ModeHeavy {
		return e.dispatchHeavy(ctx, jobID, req)
	}
	return e.dispatchLightweight(ctx, jobID, req)
}

// dispatchLightweight는 작업을 워커 풀 큐에 넣고 결과를 기다린다.
// 결과 채널은 버퍼 크기 1로 만들어, 호출자가 ctx 취소로 먼저 이탈하더라도
// 워커 고루틴이 결과 전송 시 블로킹되어 누수(goroutine leak)되지 않도록 한다.
func (e *HybridEngine) dispatchLightweight(ctx context.Context, jobID string, req WorkflowRequest) (*ExecutionResult, error) {
	resultCh := make(chan lightweightOutcome, 1)

	select {
	case e.jobQueue <- lightweightJob{ctx: ctx, jobID: jobID, request: req, resultCh: resultCh}:
	case <-ctx.Done():
		return nil, fmt.Errorf("lightweight 작업 큐잉 취소 (job_id=%s): %w", jobID, ctx.Err())
	}

	select {
	case outcome := <-resultCh:
		return outcome.result, outcome.err
	case <-ctx.Done():
		return nil, fmt.Errorf("lightweight 작업 대기 취소 (job_id=%s): %w", jobID, ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// Lightweight 모드: 세션 격리 임시 디렉토리 + os/exec
// ---------------------------------------------------------------------------

// workflowYAMLTemplate은 실제 설치된 stepflow-cli(0.13.0)의 flow 파일 스키마를 따른다.
// - steps[].component는 반드시 "/"로 시작하는 컴포넌트 경로여야 한다.
// - steps[].input은 컴포넌트 입력값(JSON을 인라인 삽입해도 유효한 YAML 상위집합).
// - output은 "$step" 참조 문법으로 마지막 스텝의 결과를 워크플로우 출력으로 연결한다.
const workflowYAMLTemplate = `# Stepflow 워크플로우 정의 (자동 생성, job_id={{.JobID}})
# 요청별 독립 임시 디렉토리에서만 생성되므로 동시 요청 간 파일 충돌이 발생하지 않는다.
name: {{.WorkflowName}}
description: "job_id={{.JobID}} 요청에 대해 자동 생성된 워크플로우"
steps:
{{- range .Tasks}}
  - id: {{.ID}}
    component: {{.Component}}
    input: {{.ParamsJSON}}
{{- end}}
{{- if .OutputStepID}}
output:
  result: { $step: {{.OutputStepID}} }
{{- end}}
`

// stepflowConfigTemplate의 plugins/routes는 실제 배포 환경에 등록된 컴포넌트 플러그인에
// 따라 달라지는 값이라 이 범용 브릿지가 대신 채워줄 수 없다. 스키마상 필수 필드이므로
// 빈 값으로 우선 생성하며, 운영 환경에서는 별도 배포 파이프라인이 plugins/routes를
// 실제 값으로 주입해야 한다. 실제 동작하는 설정 예시(examples/stepflow-config.example.yml
// 참고, stepflow-cli 0.13.0으로 validate/run까지 검증됨)의 요약:
//
//	plugins:
//	  builtin: { type: builtin }                     # 내장 컴포넌트(noop, map, iterate 등)
//	  python-analyzer:                                # 외부 MCP 서버 기반 Python 컴포넌트
//	    type: mcp                                     # (plugin type: builtin|mock|mcp|grpc|nats)
//	    command: python3
//	    args: ["-m", "stepflow_analyze_server"]
//	    env: { PYTHONDONTWRITEBYTECODE: "1" }
//	routes:
//	  "/builtin": [{ plugin: builtin }]                # 경로는 단일 세그먼트만 허용(중첩 불가)
//	  "/python-analyzer": [{ plugin: python-analyzer }]
const stepflowConfigTemplate = `# Stepflow 실행 설정 (자동 생성, job_id={{.JobID}})
# plugins/routes 실제 작성 예시는 examples/stepflow-config.example.yml 을 참고할 것.
plugins: {}
routes: {}
# 아래 storage 항목은 stepflow-cli 스키마에 속하지 않는 부가 메타데이터이며,
# 컴포넌트 구현체가 참조할 수 있도록 남겨두는 정보성 필드다.
storage:
  # 입력 데이터는 공통 저장소를 읽기 전용으로만 참조한다 (동시 실행 안전성 확보).
  input_dir: {{.SharedStorageDir}}
  # 모든 산출물은 job_id 전용 디렉토리에만 기록하여 다른 작업과 결과가 섞이지 않는다.
  output_dir: {{.OutputDir}}
`

// templateTaskView는 workflow.yaml 템플릿 렌더링용 뷰 모델이다.
// params(map)는 YAML 내에 안전하게 인라인 삽입하기 위해 사전에 JSON으로 직렬화한다.
type templateTaskView struct {
	ID         string
	Component  string
	ParamsJSON string
}

type workflowTemplateView struct {
	WorkflowName string
	JobID        string
	Tasks        []templateTaskView
	// OutputStepID는 워크플로우의 최종 출력으로 연결할 마지막 스텝의 id다.
	OutputStepID string
}

type configTemplateView struct {
	JobID            string
	SharedStorageDir string
	OutputDir        string
	SessionDir       string
}

// runLightweight는 Lightweight 모드의 전체 실행 절차를 수행한다:
//  1. os.MkdirTemp로 요청 전용 임시 세션 디렉토리 생성
//  2. 그 안에 workflow.yaml / stepflow-config.yml 동적 생성
//  3. PYTHONDONTWRITEBYTECODE=1을 강제한 채 os/exec로 `stepflow run` 실행
//  4. 결과물은 registry/outputs/{job_id}/ 로 격리
func runLightweight(ctx context.Context, jobID string, req WorkflowRequest) (*ExecutionResult, error) {
	// 세션 디렉토리는 job_id별로 완전히 독립되어야 하므로, os.MkdirTemp의 임의 접미사에
	// 더해 job_id를 패턴에 포함시켜 사람이 봐도 어떤 요청의 세션인지 즉시 식별 가능하게 한다.
	sessionDir, err := os.MkdirTemp("", fmt.Sprintf("stepflow-session-%s-", jobID))
	if err != nil {
		return nil, fmt.Errorf("세션 임시 디렉토리 생성 실패 (job_id=%s): %w", jobID, err)
	}
	// 실행이 끝나면(성공/실패 무관) 임시 세션 디렉토리는 정리한다.
	// 산출물은 이미 outputDir(공유 저장소)로 별도 기록되므로 세션 디렉토리를 남길 필요가 없다.
	defer os.RemoveAll(sessionDir)

	outputDir, err := outputDirForJob(jobID)
	if err != nil {
		return nil, err
	}

	workflowPath := filepath.Join(sessionDir, "workflow.yaml")
	configPath := filepath.Join(sessionDir, "stepflow-config.yml")
	// 실행 결과(JSON)는 세션 임시 디렉토리가 아니라 job_id 전용 공유 출력 디렉토리에 바로 기록해,
	// 세션 디렉토리 정리(defer os.RemoveAll) 이후에도 결과물이 유실되지 않도록 한다.
	resultPath := filepath.Join(outputDir, "result.json")

	if err := writeWorkflowYAML(workflowPath, jobID, req); err != nil {
		return nil, err
	}
	if err := writeStepflowConfig(configPath, jobID, sessionDir, outputDir); err != nil {
		return nil, err
	}

	if err := execStepflowRun(ctx, jobID, sessionDir, configPath, workflowPath, resultPath); err != nil {
		return nil, err
	}

	return &ExecutionResult{
		JobID:     jobID,
		Mode:      ModeLightweight,
		Status:    "completed",
		OutputDir: outputDir,
	}, nil
}

// writeWorkflowYAML은 요청 페이로드를 workflow.yaml로 렌더링하여 세션 디렉토리에 기록한다.
func writeWorkflowYAML(path, jobID string, req WorkflowRequest) error {
	taskViews := make([]templateTaskView, 0, len(req.Tasks))
	var lastStepID string
	for _, t := range req.Tasks {
		paramsJSON, err := json.Marshal(t.Params)
		if err != nil {
			return fmt.Errorf("태스크 파라미터 직렬화 실패 (job_id=%s, task_id=%s): %w", jobID, t.ID, err)
		}
		taskViews = append(taskViews, templateTaskView{ID: t.ID, Component: normalizeComponentPath(t.Action), ParamsJSON: string(paramsJSON)})
		lastStepID = t.ID
	}

	tmpl, err := template.New("workflow.yaml").Parse(workflowYAMLTemplate)
	if err != nil {
		return fmt.Errorf("workflow.yaml 템플릿 파싱 실패: %w", err)
	}

	var buf bytes.Buffer
	view := workflowTemplateView{WorkflowName: req.WorkflowName, JobID: jobID, Tasks: taskViews, OutputStepID: lastStepID}
	if err := tmpl.Execute(&buf, view); err != nil {
		return fmt.Errorf("workflow.yaml 렌더링 실패 (job_id=%s): %w", jobID, err)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("workflow.yaml 기록 실패 (job_id=%s): %w", jobID, err)
	}
	return nil
}

// normalizeComponentPath는 TaskNode.Action을 stepflow 컴포넌트 경로 규격("/"로 시작)에 맞게 변환한다.
func normalizeComponentPath(action string) string {
	if strings.HasPrefix(action, "/") {
		return action
	}
	return "/" + action
}

// writeStepflowConfig는 stepflow-config.yml을 렌더링하여 세션 디렉토리에 기록한다.
func writeStepflowConfig(path, jobID, sessionDir, outputDir string) error {
	tmpl, err := template.New("stepflow-config.yml").Parse(stepflowConfigTemplate)
	if err != nil {
		return fmt.Errorf("stepflow-config.yml 템플릿 파싱 실패: %w", err)
	}

	var buf bytes.Buffer
	view := configTemplateView{
		JobID:            jobID,
		SharedStorageDir: GetSharedStorageDir(),
		OutputDir:        outputDir,
		SessionDir:       sessionDir,
	}
	if err := tmpl.Execute(&buf, view); err != nil {
		return fmt.Errorf("stepflow-config.yml 렌더링 실패 (job_id=%s): %w", jobID, err)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("stepflow-config.yml 기록 실패 (job_id=%s): %w", jobID, err)
	}
	return nil
}

// execStepflowRun은 os/exec로 로컬 `stepflow run`을 실행한다.
// 실제 stepflow-cli(0.13.0)의 인자 규격에 맞춰 워크플로우 파일은 --flow, 결과 출력은
// --output으로 지정한다(과거 버전에 존재했던 --workflow 인자는 현재 CLI에서 거부된다).
// PYTHONDONTWRITEBYTECODE=1을 명시적으로 주입해, 동일 호스트에서 여러 세션이 동시에
// 실행되더라도 파이썬 .pyc 캐시 파일 충돌이 발생하지 않도록 한다.
func execStepflowRun(ctx context.Context, jobID, sessionDir, configPath, workflowPath, resultPath string) error {
	// 라우팅 설정이 비어있는 등의 이유로 stepflow 프로세스가 응답 없이 멈추면 워커 풀의
	// 워커 하나가 영구히 점유되어 버리므로, 상위 ctx와 무관하게 상한 타임아웃을 강제한다.
	timeout := time.Duration(envInt("STEPFLOW_RUN_TIMEOUT_SECONDS", 300)) * time.Second
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "stepflow", "run",
		"--flow", workflowPath,
		"--config", configPath,
		"--output", resultPath,
		// 워크플로우 레벨 입력은 각 스텝의 params로 대체하므로, stdin에서 입력을 기다리다
		// "Invalid JSON from stdin" 오류로 실패하지 않도록 빈 입력을 명시적으로 지정한다.
		"--input-json", "{}",
	)
	cmd.Dir = sessionDir
	cmd.Env = append(os.Environ(), PythonDontWriteBytecodeEnv)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if runCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("stepflow run 실행 시간 초과 (job_id=%s, timeout=%s): %w", jobID, timeout, err)
		}
		return fmt.Errorf("stepflow run 실행 실패 (job_id=%s): %w (stderr: %s)", jobID, err, stderr.String())
	}
	return nil
}

// envInt는 정수형 환경 변수를 파싱한다. 설정되지 않았거나 파싱에 실패하면 기본값을 사용한다.
func envInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

// ---------------------------------------------------------------------------
// Heavy 모드: K3s Pod-per-workflow
// ---------------------------------------------------------------------------

// dispatchHeavy는 client-go를 통해 K3s "default" 네임스페이스에 job_id 전용 Pod를 생성한다.
func (e *HybridEngine) dispatchHeavy(ctx context.Context, jobID string, req WorkflowRequest) (*ExecutionResult, error) {
	outputDir, err := outputDirForJob(jobID)
	if err != nil {
		return nil, err
	}

	clientset, err := e.getK8sClientset()
	if err != nil {
		return nil, fmt.Errorf("K3s clientset 획득 실패 (job_id=%s): %w", jobID, err)
	}

	podName := fmt.Sprintf("workflow-heavy-%s", jobID)
	pod := buildHeavyPodSpec(podName, jobID, req.WorkflowName)

	created, err := clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		// Pod 이름에 job_id(UUID)를 포함시켜 이론상 충돌 가능성은 배제했지만,
		// 클러스터 측 잔여 리소스 등 예외 상황을 대비해 AlreadyExists도 명시적으로 래핑한다.
		return nil, fmt.Errorf("K3s Pod 생성 실패 (job_id=%s, pod=%s): %w", jobID, podName, err)
	}

	return &ExecutionResult{
		JobID:     jobID,
		Mode:      ModeHeavy,
		Status:    "submitted",
		OutputDir: outputDir,
		PodName:   created.Name,
	}, nil
}

// buildHeavyPodSpec은 Heavy 모드용 PodSpec을 구성한다.
// 공유 볼륨은 HostPath로 마운트하여, 별도 PV/PVC 프로비저닝 없이도 로컬 K3s 노드가
// 프로젝트 실경로(/mnt/aqua_blue/romy_dev)에 직접 접근할 수 있도록 한다.
func buildHeavyPodSpec(podName, jobID, workflowName string) *corev1.Pod {
	hostPathType := corev1.HostPathDirectory
	image := os.Getenv("STEPFLOW_WORKER_IMAGE")
	if image == "" {
		image = DefaultHeavyWorkerImage
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
			Labels: map[string]string{
				"app":                 "stepflow-heavy-worker",
				"stepflow.io/job-id":  jobID,
				"stepflow.io/managed": "hybrid-engine",
			},
		},
		Spec: corev1.PodSpec{
			// 워크플로우 실행이 끝나면 Pod를 재시작하지 않는다(Pod-per-workflow 1회성 실행).
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "stepflow-worker",
					Image:   image,
					Command: []string{"stepflow", "run", "--job-id", jobID, "--workflow-name", workflowName},
					Env: []corev1.EnvVar{
						// 컨테이너 내부에서도 파이썬 바이트코드 캐시 충돌을 방지한다.
						{Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
						{Name: "STEPFLOW_JOB_ID", Value: jobID},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "shared-storage", MountPath: HeavyHostPathMount},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "shared-storage",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: HeavyHostPathMount,
							Type: &hostPathType,
						},
					},
				},
			},
		},
	}
}

// getK8sClientset은 K3s clientset을 지연 초기화(최초 1회)하여 반환한다.
// in-cluster 설정을 우선 시도하고, 실패하면 로컬 kubeconfig(~/.kube/config 또는
// KUBECONFIG 환경 변수)로 폴백한다.
func (e *HybridEngine) getK8sClientset() (kubernetes.Interface, error) {
	e.k8sOnce.Do(func() {
		cfg, err := buildKubernetesConfig()
		if err != nil {
			e.k8sInitErr = err
			return
		}
		clientset, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			e.k8sInitErr = fmt.Errorf("K3s clientset 생성 실패: %w", err)
			return
		}
		e.k8sClientset = clientset
	})
	return e.k8sClientset, e.k8sInitErr
}

// buildKubernetesConfig는 in-cluster 설정을 우선 사용하고, 실패 시 로컬 kubeconfig로 폴백한다.
func buildKubernetesConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		kubeconfigPath = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("K3s 클러스터 설정 로드 실패 (in-cluster 및 kubeconfig=%s 모두 실패): %w", kubeconfigPath, err)
	}
	return cfg, nil
}
