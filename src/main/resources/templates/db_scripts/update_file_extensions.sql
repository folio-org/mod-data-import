-- default_file_extensions:
-- change values: from MARC to MARC_BIB, add MARC_AUTHORITY and MARC_HOLDING
UPDATE ${myuniversity}_${mymodule}.default_file_extensions
SET jsonb = regexp_replace(jsonb::text, '\"MARC\"', '"MARC_BIB","MARC_AUTHORITY","MARC_HOLDING"', 'g')::jsonb
WHERE id IN ('5cf7a0e5-f359-43f5-8c96-e5df74131183','1df36889-4d3a-482e-a8c8-9c67b8245feb',
             'f445092c-94b8-408a-a9f1-5edd8b5919c9','8ffe5abc-a532-48cb-ab0e-827ea980154c',
             '6cf5568b-4d37-4d35-9116-e8a52120779d');
