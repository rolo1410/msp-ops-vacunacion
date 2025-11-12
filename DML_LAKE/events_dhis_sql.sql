SELECT (psi.eventdatavalues::json -> 'KgDzt6dPlWk'::text) ->> 'value'::text AS identificacion,
    "substring"((psi.eventdatavalues::json -> 'q8SFfIa2cmQ'::text) ->> 'value'::text, 1, 4) AS anio_aplicacion,
    "substring"((psi.eventdatavalues::json -> 'q8SFfIa2cmQ'::text) ->> 'value'::text, 6, 2) AS mes_aplicacion,
    "substring"((psi.eventdatavalues::json -> 'q8SFfIa2cmQ'::text) ->> 'value'::text, 9, 2) AS dia_aplicacion,
    ''::text AS punto_vacunacion,
    (psi.eventdatavalues::json -> 'OIkFq7hYbJg'::text) ->> 'value'::text AS unicodigo,
    ov1.name AS nom_vacuna,
    (psi.eventdatavalues::json -> 'EURmjcEcbcI'::text) ->> 'value'::text AS lote_vacuna,
    ov.name AS dosis_aplicada,
    (psi.eventdatavalues::json -> 'FMhn16LzNTT'::text) ->> 'value'::text AS nom_profesional_aplica,
    ''::text AS nom_profesional_registra,
    (psi.eventdatavalues::json -> 'fliDnqO30R4'::text) ->> 'value'::text AS edad_anios,
    (psi.eventdatavalues::json -> 'q8SFfIa2cmQ'::text) ->> 'value'::text AS fecha_vacuna,
    ov2.name AS pobla_vacuna,
    ov3.name AS grupo_riesgo,
    ov4.name AS gedad2,
    (psi.eventdatavalues::json -> 'I3YlfUStGeN'::text) ->> 'value'::text AS semanas
   FROM programstageinstance psi
     JOIN programstage ps ON psi.programstageid = ps.programstageid
     JOIN program p ON p.programid = ps.programid
     LEFT JOIN optionvalue ov ON ov.code::text = (((psi.eventdatavalues ->> 'GiRiv6kbWol'::text)::json) ->> 'value'::text)
     LEFT JOIN optionvalue ov1 ON ov1.code::text = (((psi.eventdatavalues ->> 'ziMyfC5FxY2'::text)::json) ->> 'value'::text)
     LEFT JOIN optionvalue ov2 ON ov2.code::text = (((psi.eventdatavalues ->> 'tzjSXYs7QdR'::text)::json) ->> 'value'::text)
     LEFT JOIN optionvalue ov3 ON ov3.code::text = (((psi.eventdatavalues ->> 'wmu0U6x4qxe'::text)::json) ->> 'value'::text)
     LEFT JOIN optionvalue ov4 ON ov4.code::text = (((psi.eventdatavalues ->> 'D7qVzf5LZcV'::text)::json) ->> 'value'::text)
  WHERE  psi.status::text = 'COMPLETED'::text