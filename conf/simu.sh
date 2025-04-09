# run API server
## localhost: gunicorn glodap_app:app -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8060 --timeout 120
gunicorn glodap_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8060 --keyfile conf/privkey.pem --certfile conf/fullchain.pem --reload --timeout 120

# kill process
ps -ef | grep 'glodap_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9

# pm2 start
pm2 start ./conf/ecosystem.config.js

