package object live_match {
    case class Tournament(
        name: String,
        id: Int,
        country: String,
        round: Int
    )
    case class HomeTeam(
        name: String,
        tournamentId: Int,
        isNational: Boolean
    )
    case class AwayTeam(
        name: String,
        tournamentId: Int,
        isNational: Boolean
    )
    case class MatchMetaData(
        startTimestamp: Long
        currentTimestamp: Long
        homeTeamScore: Int
        awayTeamScore: Int
    )
}