-- Seed self-hosted, relative logo URLs for PPV banks. The XMLTV output
-- resolves relative URLs against CONDUCTOR_BASE_URL, keeping this migration
-- portable while still giving Plex the fully-qualified URL it requires.
UPDATE channel
   SET logo_url = '/logos/epl-ppv-2c7b878f.png'
 WHERE enabled
   AND number BETWEEN 360 AND 361
   AND number = trunc(number)
   AND name = 'EPL PPV ' || (number::integer - 359)::text
   AND logo_url = '';

UPDATE channel
   SET logo_url = '/logos/sky-sports-premier-league-75c224d8.png'
 WHERE enabled
   AND number = 362
   AND name = 'Sky Sports Premier League'
   AND logo_url = '';

UPDATE channel
   SET logo_url = '/logos/ppv-event-6358638d.png'
 WHERE enabled
   AND number = trunc(number)
   AND (
       (
           number BETWEEN 370 AND 379
           AND name = 'PPV EVENT ' || lpad((number::integer - 369)::text, 2, '0')
       )
       OR (
           number BETWEEN 380 AND 384
           AND name = 'LIVE EVENT ' || lpad((number::integer - 379)::text, 2, '0')
       )
       OR (
           number BETWEEN 401 AND 405
           AND name = 'LIVE EVENT ' || lpad((number::integer - 395)::text, 2, '0')
       )
   )
   AND logo_url = '';

UPDATE channel
   SET logo_url = '/logos/big-ten-plus-925e1063.jpg'
 WHERE enabled
   AND number BETWEEN 385 AND 400
   AND number = trunc(number)
   AND name = 'Big Ten PPV ' || (number::integer - 384)::text
   AND logo_url = '';
