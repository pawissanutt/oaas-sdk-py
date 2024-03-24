build:
    rm dist/*
    python -m build

upload:
    python -m twine upload .\dist\*
