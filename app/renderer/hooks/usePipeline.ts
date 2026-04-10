import { usePipelineStore } from '../stores/pipelineStore';

export function usePipeline() {
  const {
    state,
    currentStage,
    progress,
    report,
    error,
    run,
    reset,
  } = usePipelineStore();

  return {
    state,
    currentStage,
    progress,
    report,
    error,
    isRunning: state === 'running',
    isSuccess: state === 'success',
    isError: state === 'error',
    run,
    reset,
  };
}
