#!/bin/bash
# Helper to build Malhar/front
# Known Issues:
#   Error: SELF_SIGNED_CERT_IN_CHAIN  
#   Workaround: npm config set ca ""
#
rm -r dist 2>/dev/null
#rm -r node_modules
npm install . && make build 
exit $?
