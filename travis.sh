#!/bin/bash
mvn test -B
cd ui
npm install .
npm test
