package bean

case class Payload(
                  push_id: Long,
                  size: Int,
                  distinct_size: Int,
                  ref: String,
                  head: String,
                  before: String,
                  commits: Commits
                  )