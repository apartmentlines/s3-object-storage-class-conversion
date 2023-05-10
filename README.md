# s3-object-storage-class-conversion

Mass convert S3 objects to another storage class.

Handles intermittent network failures, capable of running for many days in a row to convert a large number of objects -- 1.5 million objects were converted in the development of this script.

## Requirements

* Python (2.7, *might* work on 3.x)
* [s3cmd](https://s3tools.org/s3cmd) with read/write access to the bucket in question
* SQLite to store the objects to track for conversion

## Usage

Run `./app.py --help` for usage info.

## Caveats

* Assumes the bucket has at least one layer deep of folders
* Uses s3cmd class directly, which is not offically supported, so might break if they change things under the hood
