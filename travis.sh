#!/bin/bash
mvn test -B
cd front
npm install .
npm test
make build