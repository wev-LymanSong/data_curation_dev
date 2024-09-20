# DataCuration
**Data Table Curation** Module for **GE**nerative **D**ata **I**nsight(**GEDI**) Project

```Text
data_curation_dev
├── data
│   ├── requests_extraction
│   │   ├── DATA-5001.json
│   │   ├── ...	
│   │   └── DATA-6999.json
│   ├── table_source_codes
│   │   ├── we_artist.md
│   │   ├── ...	
│   │   └── wv_order.md
│   ├── spces_queue
│   │   ├── we_artist.md
│   │   ├── ...	
│   │   └── wv_order.md
│   └── specs_prod
│       ├── we_artist.md
│       ├── ...
│       └── wv_order.md
├── tools
│   ├── connectors
│   │   ├── databricks_connector.py
│   │   ├── github_repo_connector.py
│   │   ├── jira_connector.py
│   │   └── notion_connector.py
│   ├── dataset_generators
│   │   └── request_dataset.py
│   └── utils
│       ├── md2notion_uploader.py
│       ├── parser_utils.py
│       ├── table_generator.py
│       └── table_utils.py
├── configurations.py
├── editor_main.py
├── prompt_templates.py
├── requirements.txt
├── semantic_info_generator.py
├── specification_builder.py
└── static_data_collector.py
```
