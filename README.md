# Project for NSU.CS Software Design
## Epub web-reader 
Elastic search part for searching on website

...in progress
## How to build project
#### Create new folder for project

  `mkdir digital-library`

  `cd digital-library`

#### Clone all needed repositores

  `git clone git@github.com:Digital-Biblioteka/library-search.git`

  `git clone git@github.com:Digital-Biblioteka/library_frontend.git`

  `git clone git@github.com:Digital-Biblioteka/library-backend.git`

#### from the root folder (digital-library) run:

  ` docker-compose -f library-backend/docker-compose.yml -f library-search/docker-compose.yml `
