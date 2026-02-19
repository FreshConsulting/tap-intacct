def process_catalog(streams: list, fix_data: dict) -> list:
    fix_lookup = {}
    dismiss_tables = set()
    fix_key_lookup = {}

    # Parse Fix Configuration
    for table_entry in fix_data.get("tables", []):
        for table_name, table_rules in table_entry.items():

            if table_name == "DISMISS_TABLES":
                dismiss_tables.update(table_rules)

            elif table_name == "FIX_KEY_FIELD":
                for item in table_rules:
                    for tbl, key_field in item.items():
                        fix_key_lookup[tbl] = key_field

            else:
                fix_lookup[table_name] = table_rules

    # Schema Cleaning
    def clean_schema(obj):
        if isinstance(obj, dict):

            if "anyOf" in obj and isinstance(obj["anyOf"], list) and obj["anyOf"]:
                first_option = obj["anyOf"][0]
                obj.clear()
                obj.update(first_option)

            for key, value in list(obj.items()):
                if key == "type" and isinstance(value, list) and len(value) > 2:
                    obj[key] = value[:2]
                else:
                    clean_schema(value)

        elif isinstance(obj, list):
            for item in obj:
                clean_schema(item)

    # Apply Type Fixes
    def apply_type_fixes(stream_dict):
        stream_name = stream_dict.get("stream")

        if stream_name not in fix_lookup:
            return

        table_rules = fix_lookup[stream_name]
        properties = stream_dict.get("schema", {}).get("properties", {})

        for field_name, type_changes in table_rules.items():
            if field_name not in properties:
                continue

            field_schema = properties[field_name]
            field_types = field_schema.get("type")

            if not isinstance(field_types, list):
                continue

            for old_type, new_type in type_changes.items():
                field_schema["type"] = [
                    new_type if t == old_type else t
                    for t in field_types
                ]

    # Update Metadata
    def update_metadata(stream_dict):
        stream_name = stream_dict.get("stream")
        metadata_list = stream_dict.setdefault("metadata", [])

        # Ensure property metadata is selected
        for entry in metadata_list:
            breadcrumb = entry.get("breadcrumb", [])

            if len(breadcrumb) == 2 and breadcrumb[0] == "properties":
                entry.setdefault("metadata", {})["selected"] = True

            if len(breadcrumb) == 0:
                entry.setdefault("metadata", {})["selected"] = True

        # Inject table key if configured
        if stream_name in fix_key_lookup:
            key_field = fix_key_lookup[stream_name]

            root_meta = next(
                (e for e in metadata_list if e.get("breadcrumb", []) == []),
                None
            )

            if not root_meta:
                root_meta = {"breadcrumb": [], "metadata": {}}
                metadata_list.append(root_meta)

            root_meta.setdefault("metadata", {})
            root_meta["metadata"]["table-key-properties"] = [key_field]
            root_meta["metadata"]["selected"] = True

    # Main Processing
    processed_streams = []

    for stream_dict in streams:
        stream_name = stream_dict.get("stream")

        if not stream_name:
            continue

        if stream_name in dismiss_tables:
            continue

        clean_schema(stream_dict)
        apply_type_fixes(stream_dict)
        update_metadata(stream_dict)

        processed_streams.append(stream_dict)

    return processed_streams
