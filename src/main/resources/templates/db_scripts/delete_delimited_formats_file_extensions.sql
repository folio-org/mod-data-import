-- delete csv, tsv, txt file extensions from default_file_extensions
DELETE FROM ${myuniversity}_${mymodule}.default_file_extensions
WHERE id IN ('f375ffe9-b00b-4786-b0a8-cd99f93e5aab', '4a052746-5a21-44d1-a2ed-472c5747a488', 'bd7ccd60-0528-4abf-88cc-a89b0b127be1');

-- delete csv, tsv, txt file extensions from file_extensions
DELETE FROM ${myuniversity}_${mymodule}.file_extensions
WHERE id IN ('f375ffe9-b00b-4786-b0a8-cd99f93e5aab', '4a052746-5a21-44d1-a2ed-472c5747a488', 'bd7ccd60-0528-4abf-88cc-a89b0b127be1');
