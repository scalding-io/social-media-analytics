package io.scalding.approximations.BloomFilter

import com.twitter.algebird.BloomFilter
import com.twitter.scalding.typed.TDsl
import com.twitter.scalding.{Csv, Tsv, Job, Args}

/**
 * @author Stefano Galarraga - http://scalding.io
 *
 *         This example extracts the list of posts grouped by User for the users in the
 *         specified age range
 */
class BFStackExchangeUsersPostExtractor(args: Args) extends Job(args) {
  import TDsl._

  val size = 100000
  val fpProb = 0.01

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)

  val minAge = args("minAge").toInt
  val maxAge = args("maxAge").toInt

  val posts = Tsv(args("posts"), Post.fields, true)
    .read
    .toTypedPipe[Post.TupleType]( Post.fields )
    .map { Post.fromTuple }

  val usersInAgeRange = Tsv(args("users"), User.fields, true)
    .read
    .filter('Age) { age: Int =>  (minAge <= age) && (age <= maxAge)  }
    .toTypedPipe[User.TupleType]( User.fields )
    .map { User.fromTuple }


  val userBF = usersInAgeRange
    .map { user => bloomFilterMonoid.create(user.id.toString) }
    .sum

  val preFilteredPosts = posts.filterWithValue(userBF) { (post, bfOption) =>
    bfOption map { _.contains( post.ownerUserID.toString ).isTrue } getOrElse false
  }

  preFilteredPosts
    .groupBy { _.ownerUserID }
    .join { usersInAgeRange.groupBy { _.id  } }
    .mapValues { postAndUser: (Post, User) =>
      val (post, user) = postAndUser
      (user.id, user.displayName, user.accountId, user.age, post.id, post.parentID, post.creationDate)
    }
    .values
    .toPipe( 'userId, 'displayName, 'accountId, 'age, 'postId, 'postParentID, 'postCreationDate )
    .write( Csv(args("output")) )
}


object Post {
  type TupleType = (Long, Long, Long, Long, String, Long, Long, String, String)

  val fields = List('Id, 'PostTypeID, 'ParentID, 'OwnerUserID, 'CreationDate,
    'WiewCount, 'FavoriteCount, 'Tags, 'Keywords)

  def fromTuple(tuple: TupleType) = Post(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6, tuple._7, tuple._8, tuple._9)

}

case class Post(id: Long, postTypeID: Long, parentID: Long, ownerUserID: Long, creationDate: String,
                wiewCount: Long, favoriteCount: Long, tags: String, keywords: String)

object User {
  val inputFields = List('Id, 'Reputation, 'CreationDate,
    'DisplayName, 'LastAccessDate, 'WebsiteUrl,
    'Location, 'AboutMe, 'Views, 'UpVotes,
    'DownVotes, 'EmailHash, 'AccountId, 'Age)

  val fields = List('Id, 'DisplayName, 'LastAccessDate, 'AccountId, 'Age)

  type TupleType = (Long, String, String, Long, Int)

  def fromTuple(tuple: TupleType) = User(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5)
}

case class User(id: Long, displayName: String, lastAccessDate: String, accountId: Long, age: Int)

