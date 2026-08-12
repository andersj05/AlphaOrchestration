"""Thin Alpha-owned adapter over KernelCubed's public runtime contract.

This module never imports vLLM.  KernelCubed stays responsible for the one
shared engine, while Alpha owns full prompt construction, request IDs,
transcripts, retries, policy, and persistence.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import aclosing
from pathlib import Path
from typing import Any

from alpha_orchestration.ports import EngineDelta, EngineRequest


class EngineOverloaded(RuntimeError):
    pass


class EngineCallerError(ValueError):
    pass


class KernelCubedAdapter:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    @classmethod
    def from_model(
        cls,
        model: Path | str,
        *,
        max_context: int = 4096,
        max_sessions: int = 8,
        max_num_seqs: int = 4,
        max_pending_requests: int = 32,
        max_num_batched_tokens: int = 2048,
        gpu_memory_utilization: float = 0.50,
    ) -> KernelCubedAdapter:
        try:
            from kernelcubed.vllm_runtime import VLLMRuntime
        except ImportError as exc:
            raise RuntimeError(
                "KernelCubed is not importable; add /home/base/KernelCubed to PYTHONPATH "
                "or install it editable with --no-deps in the validated engine environment"
            ) from exc
        runtime = VLLMRuntime.from_model(
            str(model),
            max_context=max_context,
            max_sessions=max_sessions,
            max_num_seqs=max_num_seqs,
            max_pending_requests=max_pending_requests,
            max_num_batched_tokens=max_num_batched_tokens,
            gpu_memory_utilization=gpu_memory_utilization,
            cudagraph_capture_size=max_num_seqs,
            enable_prefix_caching=False,
            enforce_eager=False,
        )
        return cls(runtime)

    def create_session(self, session_id: str) -> str:
        return str(self.runtime.create_session(session_id))

    async def stream(self, request: EngineRequest) -> AsyncIterator[EngineDelta]:
        try:
            from kernelcubed.vllm_runtime import VLLMSampling
        except ImportError as exc:
            raise RuntimeError("KernelCubed is not importable") from exc

        controls = VLLMSampling(
            temperature=float(request.sampling.get("temperature", 0.0)),
            top_p=float(request.sampling.get("top_p", 1.0)),
            top_k=int(request.sampling.get("top_k", 0)),
            seed=int(request.sampling.get("seed", 0)),
            ignore_eos=False,
        )
        try:
            raw_stream = self.runtime.stream_generate(
                request.session_id,
                request.prompt_ids,
                max_new_tokens=request.max_new_tokens,
                sampling=controls,
                request_id=request.request_id,
            )
            async with aclosing(raw_stream) as chunks:
                async for chunk in chunks:
                    telemetry: dict[str, Any] = {}
                    result = chunk.result
                    if result is not None:
                        telemetry = {
                            "prompt_tokens": result.prompt_tokens,
                            "cached_prompt_tokens": result.cached_prompt_tokens,
                            "queue_wait_seconds": result.queue_wait_seconds,
                            "engine_queue_wait_seconds": result.engine_queue_wait_seconds,
                            "time_to_first_token_seconds": result.time_to_first_token_seconds,
                            "generation_seconds": result.generation_seconds,
                            "total_seconds": result.total_seconds,
                            "output_ids": list(result.output_ids),
                        }
                    yield EngineDelta(
                        request_id=chunk.request_id,
                        session_id=chunk.session_id,
                        delta_ids=tuple(chunk.delta_ids),
                        delta_text=chunk.delta_text,
                        generated_tokens=chunk.generated_tokens,
                        finished=chunk.finished,
                        finish_reason=(result.finish_reason if result is not None else chunk.finish_reason),
                        telemetry=telemetry,
                    )
        except ValueError as exc:
            raise EngineCallerError(str(exc)) from exc
        except RuntimeError as exc:
            if "capacity reached" in str(exc):
                raise EngineOverloaded(str(exc)) from exc
            raise

    async def cancel(self, request_id: str) -> bool:
        return bool(await self.runtime.cancel(request_id))

    async def close(self) -> None:
        await self.runtime.close()
