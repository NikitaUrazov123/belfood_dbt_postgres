with source as (
      select * from {{ source('Stage1CUpp', 'С_ДоговорыКонтрагентов') }}
),
renamed_and_cast as (
    select
    "ValyutaVzaimoraschetov" as "ВалютаВзаиморасчетов"
    ,"VedenieVzaimoraschetov" as "ВедениеВзаиморасчетов"
    ,"VidVzaimoraschetov" as "ВидВзаиморасчетов"
    ,"VidUsloviyDogovora" as "ВидУсловийДоговора"
    ,"DerzhatRezervBezOplatyOgranichennoeVremya" as "ДержатьРезервБезОплатыОграниченноеВремя"
    ,"DopustimayaSummaZadolzhennosti" as "ДопустимаяСуммаЗадолженности"
    ,"DopustimoeCHisloDneyZadolzhennosti" as "ДопустимоеЧислоДнейЗадолженности"
    ,"Kommentariy" as "Комментарий"
    ,"KontrolirovatSummuZadolzhennosti" as "КонтролироватьСуммуЗадолженности"
    ,"KontrolirovatCHisloDneyZadolzhennosti" as "КонтролироватьЧислоДнейЗадолженности"
    ,"ObosoblennyyUchetTovarovPoZakazamPokupateley" as "ОбособленныйУчетТоваровПоЗаказамПокупателей"
    ,"Organizatsiya" as "Организация"
    ,"ProtsentKomissionnogoVoznagrazhdeniya" as "ПроцентКомиссионногоВознаграждения"
    ,"ProtsentPredoplaty" as "ПроцентПредоплаты"
    ,"SposobRaschetaKomissionnogoVoznagrazhdeniya" as "СпособРасчетаКомиссионногоВознаграждения"
    ,"TipTSen" as "ТипЦен"
    ,"CHisloDneyRezervaBezOplaty" as "ЧислоДнейРезерваБезОплаты"
    ,"VidDogovora" as "ВидДоговора"
    ,"UchetAgentskogoNDS" as "УчетАгентскогоНДС"
    ,"VidAgentskogoDogovora" as "ВидАгентскогоДоговора"
    ,"KontrolirovatDenezhnyeSredstvaKomitenta" as "КонтролироватьДенежныеСредстваКомитента"
    ,"RaschetyVUslovnykhEdinitsakh" as "РасчетыВУсловныхЕдиницах"
    ,"Data" as "Дата"
    ,"Nomer" as "Номер"
    ,"RealizatsiyaNaEksport" as "РеализацияНаЭкспорт"
    ,"VestiPoDokumentamRaschetovSKontragentom" as "ВестиПоДокументамРасчетовСКонтрагентом"
    ,"OsnovnoyProekt" as "ОсновноыПроект"
    ,"OsnovnayaStatyaDvizheniyaDenezhnykhSredstv" as "ОсновнаяСтатяДвиженияДенежныхСредств"
    ,"SrokDeystviya" as "СрокДеыствия"
    ,"PoryadokRegistratsiiSchetovFakturNaAvansPoDogovoru" as "ПорядокРегистрацииСчетовФактурНаАвансПоДоговору"
    ,"NaimenovanieDlyaSchetaFakturyNaAvans" as "НаименованиеДляСчетаФактурыНаАванс"
    ,"NalogovyyAgentPoOplate" as "НалоговыйАгентПоОплате"
    ,"CHisloDneyZadolzhennostiPoDogovoru" as "ЧислоДнейЗадолженностиПоДоговору"
    ,"DopOtsrochka" as "ДопОтсрочка"
    ,"NDSPoTS" as "НДСПоТС"
    ,"VyvoditTSenuProizvoditelyaVTTN" as "ВыводитьценуПроизводителяВТТН"
    ,"yui_Ispolnitel" as "юиИсполнител"
    ,"yui_Podrazdelenie" as "юиПодразделение"
    ,"yui_StatyaByudzheta" as "юиСтатяБюджета"
    ,"BezaktseptnoeSpisanie" as "БезакцептноеСписание"
    ,"KolichestvoDneyAktsepta" as "КоличествоДнеякцепта"
    ,"yu_StavkaPeni" as "юСтавкаПени"
    ,"NaznacheniePlatezha" as "НазначениеПлатежа"
    ,"SummaKontrakta" as "СуммаКонтракта"
    ,"Predopredelennyy" as "Предопределенный"
    ,"Ssylka" as "Ссылка"
    ,"PometkaUdaleniya" as "ПометкаУдаления"
    ,"EtoGruppa" as "ЭтоГруппа"
    ,"Vladelets" as "Владелец"
    ,"Roditel" as "Родитель"
    ,"Naimenovanie" as "Наименование"
    ,"Kod" as "Код"
    ,"ParametrNaimenovanie" as "ПараметрНаименование"
    ,"ValyutaVzaimoraschetovGuid" as "ВалютаВзаиморасчетовГуид"
    ,"VidVzaimoraschetovGuid" as "ВидВзаиморасчетовГуид"
    ,"OrganizatsiyaGuid" as "ОрганизацияГуид"
    ,"TipTSenGuid" as "ТипценГуид"
    ,"OsnovnoyProektGuid" as "ОсновноыПроектГуид"
    ,"yui_IspolnitelGuid" as "юиИсполнителГуид"
    ,"yui_PodrazdelenieGuid" as "юиПодразделениеГуид"
    ,"yui_StatyaByudzhetaGuid" as "юиСтатяБюджетаГуид"
    ,"SsylkaGuid" as "СсылкаГуид"
    ,"VladeletsGuid" as "ВладелецГуид"
    ,"RoditelGuid" as "РодительГуид"
    ,"__Partition"
    from source
),

filtred as 
(
    select * from renamed_and_cast
    where "ПометкаУдаления" = False
)

select * from filtred
  