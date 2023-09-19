echo [$(date)] : "START"
echo [$(date)]  : "CREATE A VIRTUAL ENV"
virtualenv venv --python=python3.9
echo [$(date)] "Activationg the dev envirnmnets"
. venv/bin/activate
echo [$(date)]: "installing the  dev requirements"
pip install -r requirements_dev.txt
echo [$(date)]: "END"