package bean

case class GitHubData(
                       id: Long,
                       `default`: String,
                       actor: Actor,
                       repo: Repo,
                       payload: Payload,
                       public: Boolean,
                       created_at: String
                     )