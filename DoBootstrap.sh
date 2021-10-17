if [ ! -d env ]
then
    python3 -m venv env
fi

. env/bin/activate

python3 -m pip install --upgrade pip
pip install -r requirements.txt
