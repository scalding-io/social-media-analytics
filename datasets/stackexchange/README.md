Two files are included from the Stackexchange archive

- Posts.tsv

This file has all the questions and answers from http://scifi.stackexchange.com/ in a *preprocessed* form

The original data field documentation is at http://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

Only rows from the original XML data file whose PostTypeID is 1 or 2 is kept.

There are 9 tab separated fields:

* ID
* PostTypeID
* ParentID
* OwnerUserID
* CreationDate
* ViewCount
* FavoriteCount
* Tags
* Keywords

Example code in this repository shows how to process and use HLL, BF and Count-min Sketch probabilistic algorithms in Scala / scalding using the library `algebird`


- Users.tsv

This file contains the list of users

 Fields are:

* Id
* Reputation
* CreationDate
* DisplayName
* LastAccessDate
* WebsiteUrl
* Location
* AboutMe
* Views
* UpVotes
* DownVotes
* EmailHash (now always blank)
* AccountId
* Age
