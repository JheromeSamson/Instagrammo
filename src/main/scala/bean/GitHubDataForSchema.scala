package bean

case class GitHubDataForSchema(
                      id: BigInt,
                      `default`: String,
                      actor: Actor,
                      repo: String,
                      payload: Payload,
                      `public`: Boolean,
                      created_at: String,
                      org: String
                     )
