package bean

case class GitHubData(
                       id: String,
                       `type`: String,
                       actor: Actor,
                       repo: String,
                       payload: Payload,
                       publicField: Boolean,
                       created_at: String,
                       org: String
                     )