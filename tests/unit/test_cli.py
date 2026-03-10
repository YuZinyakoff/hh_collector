from hhru_platform.interfaces.cli.main import main


def test_cli_help_returns_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr("sys.argv", ["hhru-platform"])
    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "health-check" in captured.out
