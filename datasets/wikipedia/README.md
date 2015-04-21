The data file is for all Wikipedia page revisions (in TSV format).

In the analysis in the book we excluded records whose user ID == 0, since
Wikipedia indicates these belong to "bookkeeping" scripts.

# Download

    echo Downloading 6.28 GBytes of wikipedia data
    wget --no-check-certificate https://googledrive.com/host/0B37f6hUX-aN5RGlKUjFSNUl1NVk/wikipedia-revisions.gz -O wikipedia-revisions.gz

`wikipedia-revisions-sample.tsv` contains a 100K line sample

The fields are the following:

#  wikipedia-revisions schema

    'ContributorID
    'ContributorUserName
    'RevisionID
    'DateTime

# Sample data

| ContributorID | ContributorUserName | RevisionID     | DateTime |
| -------------:| -------------------:| --------------:| --------:|
| 99	        | RoseParks	          | 233192	       | 2001-01-21T02:12:21Z |
| 0	            | Conversion script	  | 862220	       | 2002-02-25T15:43:11Z |
| 7543	        | Ams80	              | 15898945	   | 2003-04-25T22:18:38Z |
| 516514	    | Nzd	              | 56681914	   | 2006-06-03T16:55:41Z |
| 750223	    | Rory096	          | 74466685	   | 2006-09-08T04:16:04Z |
| 4477979	    | Ngaiklin	          | 133180268	   | 2007-05-24T14:41:58Z |