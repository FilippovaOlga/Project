#/bin/bash
# rm sber.db
# unzip data.zip '*01032021*'
# unzip -o data.zip
mkdir -p archive
python3 main.py
# for FILENAME in *.xlsx; do mv $FILENAME ./archive/$FILENAME.backup; done
# for FILENAME in transactions*.txt; do mv $FILENAME ./archive/$FILENAME.backup; done
