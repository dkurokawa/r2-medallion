/**
 * Stub for the `cloudflare:workers` runtime module.
 *
 * `src/workflow.ts` extends `WorkflowEntrypoint` from this runtime-only
 * module, which has no node implementation — vitest cannot resolve it, so
 * collection fails before any test runs unless something is aliased in.
 *
 * Only the class shape matters here: tests that import `runChunk` (a free
 * function, unrelated to the class) still transitively load this module
 * because it's imported at the top of workflow.ts. They never construct a
 * `MedallionDayWorkflow` instance.
 */

export class WorkflowEntrypoint<Env = unknown, T = unknown> {
  protected ctx: ExecutionContext;
  protected env: Env;

  constructor(ctx: ExecutionContext, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }
}
