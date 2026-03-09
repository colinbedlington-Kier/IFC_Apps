from backend.project_tables import get_tables_for_project_slug, load_project_tables_by_slug


def test_load_project_tables_from_dict_shape():
    config = '{"alpha": {"table": "rooms_alpha"}, "beta": {"tables": ["rooms_beta", "states_beta"]}}'
    mapping = load_project_tables_by_slug(config)
    assert mapping["alpha"] == ["rooms_alpha"]
    assert mapping["beta"] == ["rooms_beta", "states_beta"]


def test_load_project_tables_from_list_shape():
    config = '[{"slug": "alpha", "sql_table": "rooms_alpha"}, {"slug": "beta", "tables": ["rooms_beta"]}]'
    mapping = load_project_tables_by_slug(config)
    assert mapping == {"alpha": ["rooms_alpha"], "beta": ["rooms_beta"]}


def test_get_tables_for_project_slug_missing_or_invalid():
    assert get_tables_for_project_slug("", '{"alpha": {"table": "rooms_alpha"}}') is None
    assert get_tables_for_project_slug("alpha", "{not-json") is None
    assert get_tables_for_project_slug("gamma", '{"alpha": {"table": "rooms_alpha"}}') is None
