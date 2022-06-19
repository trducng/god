god - a collaborative data management system
============================================

`god` (git of data) is a distributed and extensible data management system. Main
features:

- Support data version control
- Support git-like interface: commit, checkout/merge branch, push/pull, clone
- Support storing data to object storages (currently s3, expect gcs...)
- Functionalities can be augmented with extension (e.g. add tags to files...)
- Advocate for disentangling data from code
- Expect soon: FUSE, data streaming, partial dataset download, inotify, improved
  speed, more lightweight, more feedback

Install: `pip install god`

Usage
-----

This section provides a crude overview of how to use `god`. It is hopefully
self-explanatory.

```
$ # Initiate a data repo
$ mkdir god-sample; cd god-sample
$ god init

$ # Add data
$ echo "main branch abc 1st line" >> abc
$ god add .
$ god commit -m "1st commit"

$ # Checkout different branches and update some files
$ god checkout --new other-branch
$ echo "main branch abc 2nd line" >> abc
$ god add .
$ god commit -m "1st other-branch commit"

$ # Merge branch back to main branch
$ god checkout main
$ god merge other-branch

$ # Set a remote named origin, and set it as default
$ god remote set origin "s3://bucket_name/optional_prefix"

$ # Push the data to the specified bucket in a way that other people can clone
$ god push

$ # Other people can clone
$ god clone output-folder/ s3://bucket_name/optional_prefix
```

Comparision to some other popular tools
--------------------------

The below systems inspire `god`. However, there are some large differences
in direction between `god` and each of these tools:

- **git**: does not handle large binary files well.
- **git-lfs**: requires a dedicated running server for centralized
  communication; also living inside git makes data more likely to entangle with
  code (more suitable for managing game assets, rather than GBs of large
  dataset).
- **dvc**: does not have built-in version control; living inside git make it
  more likely to have entangled code with data; it is designed to manage other
  binary assets like weights, training logs... while `god` is a more
  dataset-focused tool.
- **clearml-data**: entangles with the clearml MLOps platform, while we want to
  provide an agnostic data management solution.
- **dolt/norm**: does not handle files, it is a drop-in mysql replacement,
  though it does greatly inspire the records extension in `god`.
- **lakefs**: requires a centralized server.

Roadmap
-------

@TODO: to be updated

Contributing
------------

@TODO: to be updated

License
------------

GPLv3
