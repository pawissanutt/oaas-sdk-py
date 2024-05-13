build:
    rm -f dist/*
    python -m build

upload:
    python -m twine upload dist/*
