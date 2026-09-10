// stepflow-hybrid bridge 서버의 진입점.
// 이 파일은 REST API 라우팅과 HTTP 요청/응답 변환만 담당하며, 하이브리드 분류·워커 풀·
// K3s Pod 디스패치 등 실제 비즈니스 로직은 전부 engine 패키지(engine/hybrid.go)에 위임한다.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"stepflow-hybrid/engine"
)

// errorResponse는 API 오류 응답의 공통 JSON 포맷이다.
type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	workerCount := envInt("LIGHTWEIGHT_WORKER_POOL_SIZE", 4)
	hybridEngine := engine.NewHybridEngine(workerCount)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/workflow/execute", handleWorkflowExecute(hybridEngine))

	addr := envOr("BRIDGE_LISTEN_ADDR", ":8090")
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// HTTP 서버는 별도 고루틴에서 구동하고, 메인 고루틴은 종료 시그널을 대기하여
	// graceful shutdown(진행 중인 요청 및 워커 풀 정리)을 수행한다.
	go func() {
		log.Printf("stepflow-hybrid bridge 서버 시작: %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 서버 실행 실패: %v", err)
		}
	}()

	waitForShutdownSignal()

	log.Println("종료 시그널 수신, graceful shutdown 시작")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 서버 종료 중 오류 발생: %v", err)
	}

	// 상시 워커 풀도 함께 정리하여, 진행 중이던 lightweight 작업이 유실 없이 마무리되도록 한다.
	hybridEngine.Shutdown()
	log.Println("stepflow-hybrid bridge 서버 종료 완료")
}

// waitForShutdownSignal은 SIGINT/SIGTERM 수신까지 블로킹한다.
func waitForShutdownSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}

// handleWorkflowExecute는 POST /api/workflow/execute 요청을 처리한다.
// 책임 분리 원칙에 따라, 이 핸들러는 HTTP 계층의 검증/직렬화만 수행하고
// 하이브리드 분류 및 실행 디스패치는 hybridEngine.Dispatch에 전적으로 위임한다.
func handleWorkflowExecute(hybridEngine *engine.HybridEngine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("허용되지 않는 HTTP 메서드입니다: %s", r.Method))
			return
		}
		defer r.Body.Close()

		var req engine.WorkflowRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields() // 저작도구 스펙과의 불일치를 조기에 발견하기 위해 엄격 파싱한다.
		if err := decoder.Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("요청 페이로드 파싱 실패: %w", err))
			return
		}
		if len(req.Tasks) == 0 {
			respondError(w, http.StatusBadRequest, fmt.Errorf("tasks 필드는 최소 1개 이상의 TaskNode를 포함해야 합니다"))
			return
		}

		result, err := hybridEngine.Dispatch(r.Context(), req)
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("워크플로우 실행 디스패치 실패: %w", err))
			return
		}

		respondJSON(w, http.StatusOK, result)
	}
}

// respondJSON은 payload를 JSON으로 직렬화하여 응답한다.
func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("JSON 응답 인코딩 실패: %v", err)
	}
}

// respondError는 오류를 로깅하고 표준화된 JSON 오류 응답을 반환한다.
func respondError(w http.ResponseWriter, status int, err error) {
	log.Printf("요청 처리 오류: %v", err)
	respondJSON(w, status, errorResponse{Error: err.Error()})
}

// envOr은 환경 변수 값을 반환하거나, 설정되지 않았을 때 기본값을 반환한다.
func envOr(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// envInt는 정수형 환경 변수를 파싱한다. 파싱 실패 시 기본값으로 안전하게 폴백한다.
func envInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("%s 환경변수 파싱 실패(%q), 기본값 %d 사용: %v", key, val, fallback, err)
		return fallback
	}
	return parsed
}
