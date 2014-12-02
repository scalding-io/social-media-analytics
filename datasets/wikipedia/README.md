The data file is for all Wikipedia page revisions (in TSV format).

In the analysis in the book we excluded records whose user ID == 0, since
Wikipedia indicates these belong to "bookkeeping" scripts.

`wikipedia-revisions-sample.tsv` contains a 100K line sample

The fields are the following:

wikipedia-revisions schema
==========================

    'ContributorID
    'ContributorUserName
    'RevisionID
    'DateTime
