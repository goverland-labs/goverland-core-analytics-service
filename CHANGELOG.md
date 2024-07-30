# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.25] - 2024-07-30

### Changed
- Sort top voters with the same vp and count by last vote's date

## [0.1.24] - 2024-05-15

### Added
- Goverland index additives

## [0.1.23] - 2024-04-12

### Changed
- Bucket calculation

## [0.1.22] - 2024-03-31

### Fixed
- Query for user endpoint

## [0.1.21] - 2024-03-31

### Added
- Month filter for user, voter endpoint

## [0.1.20] - 2024-03-27

### Added
- Month filter for proposal endpoint

## [0.1.19] - 2024-03-06

### Changed
- Update platform events library to collect nats metrics

## [0.1.18] - 2024-02-18

### Added
- Spam count for monthly new proposals

## [0.1.17] - 2024-02-18

### Added
- Totals for Vp

## [0.1.16] - 2024-01-17

### Added
- Voter buckets V2

## [0.1.15] - 2024-01-11

### Changed
- Popularity index calculation

## [0.1.14] - 2024-01-09

### Changed
- Popular index calculation

## [0.1.13] - 2023-12-15

### Added
-Popularity index calculation

## [0.1.12] - 2023-11-13

### Added 
-Ecosystem charts

## [0.1.11] - 2023-11-09

### Changed
- The proposal total calculation

## [0.1.10] - 2023-11-07

### Fixed
- Mutual daos calculation

## [0.1.9] - 2023-11-07

### Fixed
- Exclusive voters calculation

## [0.1.8] - 2023-11-06

### Fixed
- Voter buckets calculation

## [0.1.7] - 2023-11-02

### Fixed
- Order vp
- Mutual daos without voters

## [0.1.6] - 2023-11-02

### Changed
- Exclusive voters response

## [0.1.5] - 2023-11-01

### Changed
- Use mv vs table to fetch the data

## [0.1.4] - 2023-10-31

### Added
- Top voters by avg vp
- Daos where voters also participate

### Changed
- Percent succeeded proposals response
- Monthly Active users calculation

## [0.1.3] - 2023-10-23

### Added
- Percent succeeded proposals

### Fixed
- Exclusive voters for dao without votes

## [0.1.2] - 2023-10-18

### Added
- Exclusive voters, monthly proposals

## [0.1.1] - 2023-10-16

### Fixed
- Fixed type in the ch request

## [0.1.0] - 2023-10-16

### Changed
- Totally reworked clickhouse writing - write data directly instead of using nats engine

### Added
- Added optional pprof debugging in runtime

## [0.0.2] - 2023-09-30

### Changed
- Database structure

### Added
- Dao consumer

## [0.0.1] - 2023-09-08

### Added
- Initialized skeleton app 
- Added monthly active users, voter buckets requests.
