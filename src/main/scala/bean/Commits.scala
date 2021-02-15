package bean

case class Commits(
                  sha: String,
                  author: Author,
                  message: String,
                  distinct: Boolean,
                  url: String
                  )
