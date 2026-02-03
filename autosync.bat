@echo off
echo Syncing with GitHub...
git add .
git commit -m "Automatic update: %date% %time%"
git push
echo Done!
pause
