package bean

case class Payload(
                    action: String,
                    before: String,
                    comment: String,
                    commits: Array[Commits],
                    description: String,
                    distinct_size: BigInt,
                    forkee: String,
                    head: String,
                    issue: String,
                    master_branch: String,
                    member: String,
                    number: String,
                    pull_request: String,
                    push_id: BigInt,
                    pusher_type: String,
                    ref: String,
                    ref_type: String,
                    release: String,
                    size: BigInt
                  )