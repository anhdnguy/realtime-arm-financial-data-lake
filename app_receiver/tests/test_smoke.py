def test_main_imports_without_starting():
    # main.py must be importable by tests; asyncio.run sits behind the __main__ guard.
    import main

    assert callable(main.main)
