@echo off
echo ========================================
echo  MARKETEYE AIRFLOW - DOCKER ON WINDOWS
echo ========================================

echo [1/4] Arrêt des services existants...
docker-compose down

echo [2/4] Nettoyage des volumes...
docker system prune -f

echo [3/4] Démarrage des services...
docker-compose up -d

echo [4/4] Attente du démarrage...
timeout /t 30 /nobreak

echo.
echo ✅ Airflow est en cours de démarrage...
echo 🌐 Interface Web: http://localhost:8080
echo 👤 Username: admin
echo 🔑 Password: admin
echo.
echo Appuyez sur une touche pour ouvrir le navigateur...
pause >nul
start http://localhost:8080

echo.
echo Pour arrêter Airflow, exécutez: docker-compose down
echo.