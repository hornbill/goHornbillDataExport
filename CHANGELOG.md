# CHANGELOG

## 1.7.1 (5th July, 2021)

Fixes:

- Updated with latest goApiLib to enable finding the Hornbill API endpoint via the proxy

## 1.7.0 (17th May, 2021)

Changes:

- Updated for compatibility with Go 1.16.x to fix issue when running in a Docker container on Windows
- Updated Excel module to v2.4

## 1.6.0 (13th November, 2020)

Features:

- Added support for exporting and using data from XLSX files from Hornbill Reports

## 1.5.1 (16th September, 2019)

Defects Fixed:

- Fixed issue with proxy not being used to download CSV file

## 1.5.0 (2nd September, 2019)

Changes:

- Added the ability to skip inserts/updates of report records into database

## 1.4.1 (6th August, 2019)

Changes:

- Applied custom timeout value to dialler as well as client

## 1.4.0 (5th August, 2019)

Features:

- Added current download process to CLI output
- Updated client transport and dialler to support download of large reports

##Â 1.3.0 (25th July, 2019)

Features:

- Added ability to define http connection timeout, for when retrieving large reports back from Hornbill

## 1.2.0 (18th March, 2019)

Features:

- Added support for MySQL 8.x
- Added support for writing to database columns whose names contain spaces
- Added version support for cross-compiling script

## 1.1.0 (28th January, 2019)

Feature:

- Improved log output when column mappings are incorrect

Defects Fixed:

- Issue with SQL query when empty columns returned within report rows
- Path issue when tool run on some non-Windows operating systems

## 1.0.0 (10th January 2019)

Features:

- Initial Release
