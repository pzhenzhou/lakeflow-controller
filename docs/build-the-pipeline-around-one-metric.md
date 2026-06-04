# Build the Pipeline Around One Metric

A system can reach the same objective through many different designs. There is
rarely one correct architecture — there are many that work, each making different
trade-offs. That's exactly why the decisions matter: the specific trade-offs
someone chooses in practice are the clearest signal of how completely they
understand the whole system. Anyone can list techniques; knowing which ones to
spend and which to give up, and why, is the real measure of architectural
understanding.

Trade-offs only mean something against a goal, though. Optimizing for many things
at once and hoping they add up is the common mistake. The better discipline: pick
the single metric that matters most to the business, and make every layer serve
it — including the workflow orchestration layer, which too often gets designed
around its own convenience instead of the goal it exists to serve.

For a data-production pipeline, that metric is straightforward: **finish all the
work as fast as possible.** Restated at the system level, that is throughput —
how much data comes out per unit time, and whether the batch is ready when it's
needed. Other goals (fairness, cost, per-task latency) still matter, but in this
context they are constraints to respect, not the thing being optimized.

Once throughput is the goal, the rest of the work is mostly negative: **find the
things that drag it down and remove them.**

## Throughput is utilization

If the goal is maximum output per unit time, the hardware has to spend as much of
its time as possible on useful work. Throughput is the business number;
utilization is that same number seen on the machines. "Improve throughput" is
vague; "stop wasting machine time" is actionable.

Wasted time comes in two forms.

**Work held too long.** A task that runs for hours holding a big slice of the
cluster locks that capacity away and stops the pool from shrinking. A few of
these pin every node like nails — the short tasks finished long ago, but nothing
can be reclaimed.

**Work that waited too long.** Every second a task sits pending while capacity is
free is pure loss, and a task stuck at the head of a queue blocks everything
behind it.

Almost every throughput problem is one of these two: held too long, or waited
too long.

## Expose the parallelism first

A scheduler can only pack work that exists. So the orchestration layer's first
job is to expose parallelism, not hide it.

- **Declare only real dependencies.** A DAG with only the edges that genuinely
  exist lets everything else run at once. The most common throughput leak is an
  author adding an ordering constraint "to be safe" — serializing work that never
  needed it. Nothing looks broken; the pipeline just runs narrower than it could.
- **Size task granularity deliberately.** Finer tasks give more independent units
  to schedule, less to pin, and cheaper preemption — but past a point you pay in
  startup overhead and data shuffled between stages. Granularity is the lever;
  the two failure modes above are what you tune it against.

## Remove what drags throughput down

With parallelism exposed, the rest is removing drag:

- **Don't let capacity sit idle for fairness.** Treat each tenant's share as a
  floor, not a ceiling: let workloads borrow idle capacity and reclaim it with
  preemption when higher-priority work arrives. (This assumes cooperative
  tenants; if isolation is the product, fairness comes first instead.)
- **Preempt by useful output, not occupancy.** Killing an un-checkpointed job
  keeps the cluster busy but throws away its progress — utilization holds, real
  output drops. Reclaim idle borrowed capacity freely; preempt running work only
  across a real priority gap.
- **Protect the critical path.** Raw throughput (jobs per second) isn't the
  finish line; the batch's completion time is. A long task on the critical path
  deserves priority even when running short tasks instead would score better
  per second.

## A separate problem: modeling the node pool

It's worth being precise that two things often blurred together are actually
distinct problems. **Maximizing utilization** is about packing the work that
exists onto the capacity you already have — a scheduling concern. **Modeling the
node pool** is about how that capacity is shaped in the first place: how nodes are
allocated when demand rises and reclaimed when it falls. At its core this second
problem is resource allocation and recovery, and it sits underneath the
scheduler, not inside it.

The reason it deserves its own treatment is that one undifferentiated pool can't
serve workloads that behave nothing alike. The pool can instead be modeled along
a few axes of the tasks it has to hold:

- **Lifecycle** — short-lived vs long-lived vs always-on work.
- **Completion time** — how long a task is expected to occupy capacity before it
  releases it.
- **Working set** — how much resource it holds while it runs.

Modeled this way, the pool splits naturally: short tasks on elastic or spot
capacity that can grow and shrink freely; long tasks packed together on stable
capacity so the rest can drain; always-on tasks on reserved capacity outside
reclamation entirely. The hard part is proportion and dynamics — sizing each
class, and recognizing that whether a task is short or long often isn't known
until it runs. But that difficulty belongs to allocation and recovery, and is
better solved on its own terms than tangled into the scheduler's packing logic.
