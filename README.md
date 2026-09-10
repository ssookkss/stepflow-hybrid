# Stepflow Hybrid Engine & Bridge

웹 저작도구의 JSON 페이로드를 REST API로 수신하고, 공통 저장소(`SHARED_STORAGE_DIR`)와 연동하여
Stepflow 규격으로 변환·실행을 제어하는 Go 기반 동시성 안전 하이브리드 엔진.

- 타겟 환경: CachyOS(Linux), Go 1.22+, 로컬 K3s 클러스터, 공유 파일 시스템
- 상세 아키텍처/코딩 규칙: [`CLAUDE.md`](./CLAUDE.md)

## 문서

- [시퀀스 다이어그램](./docs/sequence-diagram.md) — 서버 시작/종료, 요청 접수·하이브리드 분류,
  Lightweight(상시 워커 풀 + `os/exec`)와 Heavy(K3s Pod-per-workflow) 모드의 전체 실행 흐름
- [`stepflow-config.yml` plugins/routes 설정 예시](./examples/stepflow-config.example.yml)

## 아키텍처 요약

- **Lightweight**: TaskNode 3개 이하이며 action에 `analyze`가 없는 경우 → 상시 워커 풀에서
  UUID 기반 격리 임시 디렉토리에 `workflow.yaml`/`stepflow-config.yml`을 생성 후 `os/exec`로
  로컬 `stepflow run` 실행.
- **Heavy**: TaskNode 4개 이상이거나 action에 `analyze`가 포함된 경우 → `client-go`로 K3s
  `default` 네임스페이스에 `workflow-heavy-{job_id}` Pod를 생성(공유 볼륨 HostPath 마운트).

자세한 단계별 흐름은 [시퀀스 다이어그램](./docs/sequence-diagram.md)을 참고한다.

## 디렉토리 구조

- `main.go`: REST API 서버 (`POST /api/workflow/execute`)
- `engine/hybrid.go`: 동시성 안전 분류, UUID 세션 격리, 워커 풀, K3s 공유 볼륨 디스패처
- `examples/`: `stepflow-config.yml` plugins/routes 실제 설정 예시
- `docs/`: 시퀀스 다이어그램 등 설계 문서
- `registry/`: 공유 저장소(`SHARED_STORAGE_DIR` 기본값) — 입력은 읽기 전용, 출력은 `outputs/{job_id}/`

## 빌드 및 실행

```bash
go build -o bin/bridge .
./bin/bridge

# 또는
go run main.go
```

주요 환경 변수: `SHARED_STORAGE_DIR`, `BRIDGE_LISTEN_ADDR`(기본 `:8090`),
`LIGHTWEIGHT_WORKER_POOL_SIZE`(기본 4), `STEPFLOW_RUN_TIMEOUT_SECONDS`(기본 300),
`STEPFLOW_WORKER_IMAGE`, `KUBECONFIG`.
