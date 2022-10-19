SELECT cols.column_name columna,
       o_tab.table_name tabla,
       o_tab.constraint_name
FROM all_constraints alls
JOIN user_cons_columns cols ON cols.constraint_name = alls.constraint_name
AND cols.constraint_name IN ( SELECT const.constraint_name FROM all_constraints const
                              FILTRO
                            )
JOIN all_constraints o_tab ON o_tab.constraint_name = alls.r_constraint_name