package bean

case class GitHubData(
                       id: Long,
                       `default`: String,
                       actor: Actor,
                       repo: String,
                       payload: Payload,
                       public: Boolean,
                       created_at: String,
                       org: String
                     )