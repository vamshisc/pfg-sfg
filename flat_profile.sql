WITH BaseProfile AS (
    SELECT
        CASE
            WHEN EXISTS (SELECT 1 FROM SCI_PROFILE WHERE OBJECT_NAME = CONCAT('ABC', '_PRODUCER'))
                THEN CONCAT('ABC', '_PRODUCER')
            WHEN EXISTS (SELECT 1 FROM SCI_PROFILE WHERE OBJECT_NAME = CONCAT('ABC', '_CONSUMER'))
                THEN CONCAT('ABC', '_CONSUMER')
            ELSE NULL
        END AS OBJECT_NAME,
        CASE
            WHEN EXISTS (SELECT 1 FROM SCI_PROFILE WHERE OBJECT_NAME = CONCAT('ABC', '_PRODUCER'))
                THEN 'PRODUCER'
            WHEN EXISTS (SELECT 1 FROM SCI_PROFILE WHERE OBJECT_NAME = CONCAT('ABC', '_CONSUMER'))
                THEN 'CONSUMER'
            ELSE 'UNKNOWN'
        END AS ROLE
),
Codelists AS (
    SELECT DISTINCT LIST_NAME
    FROM CODELIST_XREF_ITEMS cxi
    JOIN BaseProfile bp ON bp.ROLE = 'PRODUCER' AND cxi.SENDER_ITEM = 'ABC'
),
LatestCodelistVersions AS (
    SELECT cxv.LIST_NAME, cxv.DEFAULT_VERSION
    FROM CODELIST_XREF_VERS cxv
    JOIN Codelists cl ON cl.LIST_NAME = cxv.LIST_NAME
),
AssociatedConsumers AS (
    SELECT DISTINCT cxi.RECIEVER_ITEM AS CONSUMER_NAME
    FROM CODELIST_XREF_ITEMS cxi
    JOIN LatestCodelistVersions lcv
      ON cxi.LIST_NAME = lcv.LIST_NAME
     AND cxi.LIST_VERSION = lcv.DEFAULT_VERSION
    WHERE cxi.SENDER_ITEM = 'ABC'
)
SELECT
    sp.OBJECT_NAME                               AS "Profile Name",
    bp.ROLE                                      AS "Role",
    ac.CONSUMER_NAME                             AS "A_CON", -- New consumer column
    COALESCE(sp.EXTENDS_OBJECT_ID, 'NONE')       AS "Base Profile",
    yo.ORGANIZATION_NAME                         AS "Identity",
    sp.OBJECT_ID                                 AS "Profile Id",
    sp.OBJECT_VERSION                            AS "Profile Definition",
    sdc.OBJECT_NAME                              AS "Delivery Channel",
    sp.PACKAGING_ID                              AS "Packaging",
    COALESCE(sp.SVC_PROVIDER_ID, 'none')         AS "Provider",
    sp.PROFILE_TYPE                              AS "Profile Type",
    sp.GLN                                       AS "GLN",
    COALESCE(sp.SERVICE, 'None provided')        AS "Send",
    COALESCE(sp.ACTION, 'None provided')         AS "Respond",
    COALESCE(sp.SERVICE_TYPE, 'None provided')   AS "Request-Response",
    -- Add all your other Profile/Identity/Associated Objects fields here ...
FROM
    BaseProfile bp
LEFT JOIN SCI_PROFILE sp
    ON sp.OBJECT_NAME = bp.OBJECT_NAME
LEFT JOIN YFS_ORGANIZATION yo
    ON sp.ENTITY_ID = yo.OBJECT_ID
LEFT JOIN SCI_DELIV_CHAN sdc
    ON sp.DELIV_CHANNEL_ID = sdc.DELIVERY_CHANNEL_KEY
LEFT JOIN AssociatedConsumers ac
    ON bp.ROLE = 'PRODUCER' -- Only link consumers if it's a producer
WHERE
    bp.OBJECT_NAME IS NOT NULL
