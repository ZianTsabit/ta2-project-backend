# ta2-project-backend

run backend

```

gunicorn -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:7000 --workers 4
```
