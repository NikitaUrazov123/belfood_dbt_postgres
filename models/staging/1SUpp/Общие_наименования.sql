SELECT
    `ОбъектГуид`, `Значение`
FROM
    {{source('Stage1CUpp', 'РС_ЗначенияСвойствОбъектов')}}
WHERE
    `Свойство` = 'Общее наименование'