-- Create MySQL database with unresolved ip:port will cause serverError 501
CREATE DATABASE conv_main ENGINE = MySQL('dc02:104:236::103:3456', conv_main, 'metrika', 'password'); -- { serverError 501 }
