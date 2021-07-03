god - A collaborative data management software
==============================================

**Content**

- [Intro](#intro)
  - [Main features](#main-features)
  - [Main components](#main-components)
- [Installation](#installation)
- [Usage](#usage)
  - [Objects](#objects-file-version-control)
  - [Records](#records)
  - [Snapshots](#snapshots)
- [Comparision to some other popular tools](#comparision-to-some-other-popular-tools)
- [Roadmap](#roadmap)
- [Contributing](#contributing)

Intro
-----

`god` (stands for **g**od **o**f **d**ata) is an open-source tool focusing on
data versioning and data organization, independently from machine
learning workflow.

It operates under these assumptions:

- Each data file is a binary file
- Data files are gradually added/removed/refined and need to be tracked
- Data files should usually be grouped together to be meaningful (e.g. input
  files and label files)
- Building dataset is a collaborative process
- Data is accessible from any machine

### Main features

`god` allows:

- Trace the evolution of a data file with data versioning
- Group related data files (e.g. input, label) into record entries
- Annotate each record entry to help with searching and retrieving
- Stream data on-the-fly
- Retrieve partial dataset locally
- Share exact data files used in an experiment

### Main components

`god` provides these main components:

- Objects: these are data files, each of which indexed and versioned by `god`.
- Records: these are groupings of related data points, along with arbitrary
  tags/features helpful for relevant records to be searched.
- Snapshots: text snapshots of records


Installation
-----------

```
$ pip install god

$ # After that users can run command with executable `god`
$ god ...
```

Usage
-----

This section provides a crude overview of how to use `god`. It is hopefully
self-explanatory. Nevertheless, you can pull this repo to walk along to get more
hands-on experience with `god`'s features.

### Objects (file version control)

```
$ # Initiate a data repo
$ mkdir god-sample; cd god-sample
$ god init

$ # Add data
$ mv ../<some-files> .
$ god add .
$ god commit

$ # Push the data to `god` server (currently support local, gcp, aws)
$ god push

$ # Checkout different branches
$ god checkout -b other-branch

$ # Merge branch back to main branch
$ god checkout main
$ god merge other-branch
```

### Records

The record is simply a SQL database table, where each entry points to group of
related files (e.g. input and label). Since it is a database table, you can add
more columns to help tag records for ease of searching later.

To use record, simply supply the SQL table definition. `god` will find matched
files and populate them into database. The record table is also version
controlled.

```
$ # Supply rule to organize data files in `.godconfig.yml` file
$ vim ~/.godconfig.yml

$ # Organize data into related instances
$ god record add --apply

$ # Annotate records that relate to folder1 to have features `blurry`
$ god record update folder1 --features blurry

$ # Retrieve relevant records (example, find all records that are blurry)
$ # The output is a text file pointing to location of files that have features
$ # blurry.
$ god record search --features blurry
```

### Snapshots

The snapshot is a text file containing location of files. As output of a record
search is a text file, it can also be considered as a snapshot. You can use this
text file in your training to access to the training data. You can share this
text file with other people so they know exactly the files (and version) that
you use in your training. `god` can help with storing these text files so that
everyone can get the snapshot text file from a central location, and you don't
need to copy/paste this text file to send to other people.

```
$ # Add the snapshot text file to `god`
$ god snap add <text-file> <unique-name>

$ # Retrieve the snapshot text file
$ god snap get <unique-name>
```


Comparision to some other popular tools
--------------------------

- **git-lfs**: focuses on index binary files inside a code repo, and does not
  support other functionalities needed for data organization. It inspires the
  object component in `god`.
- **dvc**: similar to git-lfs, it does not support other functionalities needed
  for data organization. It needs `git` in order to work. Also, it is designed
  to also manage other binary assets like weights, training logs... while we
  just want to focus on the data.
- **clearml-data**: shares the same spirit as `god`, but it has to work with the
  clearml MLOps platform, while we want to provide a general solution. Also, it
  does not support funcionalities like records and snapshots.
- **dolt/norm**: `god` shares the same spirit to provide a version-control to
  organize file paths into meaningful dataset.


Roadmap
-------

@TODO: to be updated

Contributing
------------

@TODO: to be updated
