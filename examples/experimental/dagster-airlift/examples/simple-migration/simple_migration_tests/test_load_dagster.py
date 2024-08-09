def test_defs_loads(airflow_instance):
    from simple_migration.dagster_defs import defs

    assert defs

    from simple_migration.dagster_defs.migrate import defs

    assert defs

    from simple_migration.dagster_defs.observe import defs

    assert defs

    from simple_migration.dagster_defs.peer import defs

    assert defs
