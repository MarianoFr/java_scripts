/*
6.1
  Added GPRS pow storing to mysql in table gpspos
6.3
  Fixed to fill GPS time with server time when PARAM msg has no GPS time.

  Se arreglo el reporte de parametros

6.3.1
  Parche remito de trasbordo y observaciones tambo version q 10.5

6.3.2
  Parche columna patente

6.3.3
  Parche asincronizmo para obtener patente

6.3.4 03/05/2022
  Parche correccion bug insercion uTambos

6.3.5 06/05/2022
  Mensages no reconocidos guardados en tabla errores  

6.3.6 23/05/2022
  Mensages no reconocidos guardados en tabla errores  

6.3.7 26/05/2022
  La excepcion para los mensajes se pasa a todos los topicos
  Diferenciacion de modems para el momento de actualizaciones
  
6.4  
  Resolucion de parches con nuevo soft caudalimetro SIM7002 v11.3

6.5 23/05/2022
  Insersion de hr de servidor en tabla gpspos
*/ 
var mysql = require('mysql');
var mqtt = require('mqtt');
var update_sent = "0";
var last_sol = "0";
var AccuracyGlobal = 20;
var globalTag;
var fechaRecorrido;
var rx_modem;
var updatedModems = new Array(8);

var getPatente = async() => {
  var queryPatente="SELECT Patente FROM camion WHERE modem='" + rx_modem + "';";
  let results = await new Promise((resolve, reject) => con.query(queryPatente, (err, results, fields) => {
    if (err) {
      reject(err)
    } else {
      resolve(results);      
    }
  }));
  return results[0]['Patente'];
}

var getUpdatedModems = async() => {
  var queryPatente="SELECT * FROM updatedModems;";
  let results = await new Promise((resolve, reject) => con.query(queryPatente, (err, results, fields) => {
    if (err) {
      reject(err)
    } else {
      resolve(results);      
    }
  }));
  for(var i=0;i<results.length;i++)
  {
    updatedModems[i]=results[i]['modems'];
  }
  return;
}

//CREDENCIALES MYSQL
var con = mysql.createConnection(
{
  host: "localhost",
  port: 3306,
  user: "admin1",
  password: "Hola1234",
  database: "insoltechv12",
  dateStrings: true
});

//CREDENCIALES MQTT
var options = 
{
  port: 1883,
  host: 'serverqst.ml',
  clientId: 'acces_control_server_' + Math.round(Math.random() * (0- 10000) * -1) ,
  username: 'java',
  password: 'Hello4321',
  keepalive: 60,
  reconnectPeriod: 1000,
  protocolId: 'MQIsdp',
  protocolVersion: 3,
  clean: true,
  encoding: 'utf8'
};

var mqtt_client = mqtt.connect("mqtt://serverqst.ml", options);

var info_icon = "/Icons/Info.png";
var tambo_icon = "/Icons/Tambo.png";
var cip_icon = "/Icons/CIP.png";
var trasvase_icon = "/Icons/Trasvase.png";

//SE REALIZA LA CONEXION
mqtt_client.on('connect', function ()
{
  console.log("Conexión  MQTT Exitosa!");
  mqtt_client.subscribe('#', function (err) 
  {
    console.log("Subscripción exitosa!")
  });
})

//CUANDO SE RECIBE MENSAJE
mqtt_client.on('message', async function (topic, message) 
{
  if(topic.includes('cliente2'))
  {
    var msg = message.toString();
    console.log("Mensaje recibido desde -> " + topic + " Mensaje -> " + msg);
    
    if(topic.includes('cliente2/report'))
    {
      if(msg == "")
      {
        console.log("empty msg");
        return;
      }
      else
      {
        const json_parser = JSON.parse(msg);
        var modem = json_parser['md'];
        var type = json_parser['msg'];
        if(type == "ACK")
        {
          var query_ack = "UPDATE `insoltechv12`.`reconfig_time` " +
            "SET `acknowledge` = CURRENT_TIMESTAMP WHERE `solicitud` = '" + last_sol + "' AND modem = '" + modem +"';"; 
          console.log(query_ack);
          try{
            con.query(query_ack, function(err, result, fields){
              if (err) throw err;
              console.log("ACK updated");
            });
          }
          catch(err){

          }
        }
        else if(type == "NACK")
        {
          var query_ack = "UPDATE `insoltechv12`.`reconfig_time` " +
            "SET `nacknowledge` = CURRENT_TIMESTAMP WHERE `solicitud` = '" + last_sol + "' AND modem = '" + modem +"';"; 
          console.log(query_ack);
          con.query(query_ack, function(err, result, fields){
            if (err) throw err;
            console.log("NACK updated");
          });
        }
      }
    }
    else if(topic.includes('cliente2/reconfig'))
    {
      if(msg == "")
      {

      }
      else
      {
        const json_parser = JSON.parse(msg);
        var modem = json_parser['md'];
        var type = json_parser['msg'];
        var param_id = json_parser["param_id"];
        if(type == "ACK")
        {
          var query_ack = "UPDATE `insoltechv12`.`reconfig_time` " +
            "SET `acknowledge` = CURRENT_TIMESTAMP WHERE `param_id` = " + param_id +";"; 
          console.log(query_ack);
          con.query(query_ack, function(err, result, fields){
            if (err) throw err;
            console.log("ACK updated");
          });
        }
        else if(type == "NACK")
        {
          var query_ack = "UPDATE `insoltechv12`.`reconfig_time` " +
            "SET `nacknowledge` = CURRENT_TIMESTAMP WHERE `param_id` = " + param_id +";"; 
          console.log(query_ack);
          con.query(query_ack, function(err, result, fields){
            if (err) throw err;
            console.log("NACK updated");
          });
        }
      }
    }
    else if(!topic.includes('report'))
    {
      var query1;
      var query2;
      var query3;
      // Parse Json
      var rx_patente;
      const json_parser = JSON.parse(msg);
      rx_modem = json_parser['md'];
      try
      {
        await getUpdatedModems();       
      }
      catch(err)
      {
        console.log(err);  
      } 
      if(updatedModems.includes(rx_modem))
      {
        console.log("Modem NO corresponde");
        return;
      }
      else
      {
        console.log("Modem SI corresponde");
      }
      try
      {
        rx_patente = await getPatente();
        console.log(rx_patente);        
      }
      catch(err)
      {
        console.log(err);  
      } 
      if (topic == "cliente2/info")
      {
        var modem = json_parser['md'];
        var nroInfo = json_parser['fld']['nI'];
        var touridx = json_parser['fld']['tid'];
        var infono = json_parser['fld']['inf'];
        var fechaHora = json_parser['fld']['FH'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(modem=="qt-008")
          Kilometros-=10420.2;
        if(infono==2789)
        {
          fechaRecorrido=fechaHora;
        }
        if(globalTag!=tag)
        {
          globalTag = tag;
          //CREATEQUERY TO BRING INFO TEXT
          
          var queryInfo = "SELECT Infotexto FROM infoTable WHERE infono=" + infono + ";";
          try
          {  
            con.query(queryInfo, function (err, result, fields) 
            {
                if (err) throw err;
                try
                {                
                    var texto = result[0]['Infotexto'];
                    console.log(texto);
                    if(gpsFechaHora=="2021-01-01 08:00:00")
                    { //Se inserta fila info
                        if((parseFloat(GPSlat) == 0) || (parseFloat(GPSlng) == 0))
                        {
                            console.log("GPSlat is 0");
                            query1 = "INSERT INTO `insoltechv12`.`info` (`modem`, `nroInfo`,`touridx`, `infono`, `FechaHora`,"+
                            " `tag`, `patente`) VALUES ('" + modem + "', '" + nroInfo + "', '"
                            + touridx + "', '" + texto +"', '" + fechaHora + "', '" + tag + "', '" + rx_patente +  "');";

                            //Se inserta fila gpspos con ícono de info
                            query2 = "";
                        }
                        else
                        {
                            AccuracyGlobal = 20.0;
                            //Se inserta fila info
                            query1 = "INSERT INTO `insoltechv12`.`info` (`modem`, `nroInfo`,`touridx`, `infono`, `FechaHora`, `GPSlat`," 
                            +"`GPSlng`, `Velocidad`, `Kilometros`, `tag`, `patente`) VALUES ('" + modem + "', '" + nroInfo + "', '"
                            + touridx + "', '" + texto +"', '" + fechaHora + "', '" + GPSlat +"', '" + GPSlng+ "', '" + Velocidad + "', '"
                            + Kilometros + "', '" + tag + "', '" + rx_patente +  "');";

                            //Se inserta fila gpspos con ícono de info
                            query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, `mapIcon`, "
                            +"`IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" 
                            + Kilometros + "', '" + AccuracyGlobal +  "', '" + tag + "', '" + info_icon + "', 'Info', '" + rx_patente + "');";
                        }
                    }
                    else
                    { //Se inserta fila info
                        query1 = "INSERT INTO `insoltechv12`.`info` (`modem`, `nroInfo`,`touridx`, `infono`, `FechaHora`, `GPSlat`," 
                            +"`GPSlng`, `Velocidad`, `Kilometros`, `gpsTime`, `tag`, `patente`) VALUES ('" + modem + "', '" + nroInfo + "', '"
                            + touridx + "', '" + texto +"', '" + fechaHora + "', '" + GPSlat +"', '" + GPSlng+ "', '" + Velocidad + "', '"
                            + Kilometros + "', '" + gpsFechaHora + "', '" + tag + "', '" + rx_patente +  "');";

                        //Se inserta fila gpspos con ícono de info
                        query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `gpsTime`, `tag`, `mapIcon`, "
                        +"`IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" 
                        + Kilometros + "', '" + AccuracyGlobal + "', '" + gpsFechaHora + "', '" + tag + "', '" + info_icon + "', 'Info', '" + rx_patente + "');";
                    }
                    console.log(query1);
                    console.log(query2);      
                    con.query(query1, function (err, result, fields) 
                    {
                        if (err) throw err;
                        console.log("Fila 'info' insertada correctamente");
                    });
                    if((parseFloat(GPSlat)!=0) && (parseFloat(GPSlng)!=0))
                    {
                        con.query(query2, function (err, result, fields) 
                        {
                            if (err) throw err;
                            console.log("Fila 'gpspos' insertada correctamente");
                        });
                    }
                }
                catch
                {
                    var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
                    con.query(queryError, function (err, result, fields) 
                    {
                        if (err) throw err;
                        console.log("Mensaje irreconocido insertado correctamente");
                    });
                } 
            });
          }
          catch
          {
              var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
              con.query(queryError, function (err, result, fields) 
              {
                  if (err) throw err;
                  console.log("Mensaje irreconocido insertado correctamente");
              });
          }
        }
      } 
      if (topic == "cliente2/recorrido")
      {
        var modem = json_parser['md'];
        var nroRecorrido = json_parser['fld']['nR'];
        var touridx = json_parser['fld']['tid'];
        var nroCamion = json_parser['fld']['uNbr'];
        var nroCuenta = json_parser['fld']['nCta'];  
        var nroAcoplado = json_parser['fld']['trailN'];
        var remito = json_parser['fld']['rmt'];
        var recorrido = json_parser['fld']['rcr'];
        var cantidadTotal = json_parser['fld']['drv'];
        var contadorMuestras = json_parser['fld']['CT'];
        var fechaHoraInicio = fechaRecorrido;//json_parser['fld']['FHI'];
        var fechaHoraFin = json_parser['fld']['FHF'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(modem=="qt-008")
          Kilometros-=10420.2;
        if(globalTag!=tag)
        {
          globalTag = tag;
          if(gpsFechaHora=="2021-01-01 08:00:00")
          {
            query1 = "INSERT INTO `insoltechv12`.`recorrido` (`modem`,`nroRecorrido`,`touridx`, `nroCamion`,`nroCuenta`, "
              + "`nroAcoplado`,`remito`,`recorrido`,`FechaHoraInicio`, `FechaHoraFin`,`cantidadTotal`,`contadorMuestras`, "
              + "`tag`, `patente`) VALUES ('"
              + modem + "', '" + nroRecorrido +"', '" + touridx + "', '" + nroCamion + "', '" + nroCuenta + "', '"
              + nroAcoplado + "', '" + remito + "', '" + recorrido + "', '" + fechaHoraInicio + "', '"+ fechaHoraFin +"', '"
              + cantidadTotal + "', '" + contadorMuestras + "', '" + tag + "', '" + rx_patente +  "');";
          }
          else
          {
            query1 = "INSERT INTO `insoltechv12`.`recorrido` (`modem`,`nroRecorrido`,`touridx`, `nroCamion`,`nroCuenta`, "
              + "`nroAcoplado`,`remito`,`recorrido`,`FechaHoraInicio`, `FechaHoraFin`,`cantidadTotal`,`contadorMuestras`, "
              + "`GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `gpsTime`, `tag`, `patente`) VALUES ('"
              + modem + "', '" + nroRecorrido +"', '" + touridx + "', '" + nroCamion + "', '" + nroCuenta + "', '"
              + nroAcoplado + "', '" + remito + "', '" + recorrido + "', '" + fechaHoraInicio + "', '"+ fechaHoraFin +"', '"
              + cantidadTotal + "', '" + contadorMuestras + "', '"+ GPSlat +"', '" + GPSlng+ "', '" + Velocidad + "', '"
              + Kilometros + "', '" + gpsFechaHora + "', '"+ tag + "', '" + rx_patente +  "');";		
          }
          console.log(query1);
          try
          {
            con.query(query1, function (err, result, fields) 
            {
                if (err) throw err;
                console.log("Fila 'recorrido' insertado correctamente");
            });
          }
          catch
          {
            var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
            con.query(queryError, function (err, result, fields) 
            {
                if (err) throw err;
                console.log("Mensaje irreconocido insertado correctamente");
            });
          }
        }
      }//Ok
      if (topic == "cliente2/tambo")
      {
        var modem = json_parser['md'];
        var nroTambo = json_parser['fld']['nT'];
        var touridx = json_parser['fld']['tid'];
        var remito = json_parser['fld']['rmt'];
        var codigoTambero = json_parser['fld']['cTm'];
        var cantidadTotal = json_parser['fld']['CT'];  
        var compA = json_parser['fld']['cA']; 
        var observaciones = json_parser['fld']['obs'];
        var cantidadA = json_parser['fld']['cnA'];
        var compB = json_parser['fld']['cB'];
        var cantidadB = json_parser['fld']['cnB'];
        var compC = json_parser['fld']['cC'];
        var cantidadC = json_parser['fld']['cnC'];
        var tempAver = json_parser['fld']['tAv'];
        var tempMin = json_parser['fld']['tMn'];
        var tempMax = json_parser['fld']['tMx'];
        var status = json_parser['fld']['stt'];
        var litrosTmuestra = json_parser['fld']['LTM'];
        var desaInicial = json_parser['fld']['daI'];
        var desaFinal = json_parser['fld']['daF'];
        var constanteE = json_parser['fld']['ctE'];
        let constanteC = json_parser['fld']['ctC'];//var constanteC = json_parser['fld']['ctC'];        
        var sampleID = json_parser['fld']['smp'];
        var fechaHoraInicio = json_parser['fld']['FHI'];
        if(fechaHoraInicio)
            fechaHoraInicio="20"+constanteC.substring(0,2)+"-"+constanteC.substring(2,4)+"-"+constanteC.substring(4,6)+" "+constanteC.substring(6,8)+":"+constanteC.substring(8,10)+":"+"00";
        var fechaHoraFin = json_parser['fld']['FHF'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(modem=="qt-008")
          Kilometros-=10420.2;
        if(globalTag!=tag)
        {
          globalTag = tag;
          if(gpsFechaHora=="2021-01-01 08:00:00")
          {
            if((parseFloat(GPSlat) == 0) || (parseFloat(GPSlng) == 0))
            {
              query1 = "INSERT INTO `insoltechv12`.`tambo` ( `modem`, `nroTambo`, `touridx`, `remito`, `observaciones`, `codigoTambero`, `cantidadTotal`, `compa`,"
                + " `cantidadA`, `compb`, `cantidadB`, `compc`, `cantidadC`, `tempaver`, `tempmin`, `tempmax`, `status`, `LTM`, `desaInicial`, `desaFinal`,"
                + " `constanteE`, `constanteC`, `SMP`, `FechaHoraInicio`, `FechaHoraFin`, `tag`)"
                + "VALUES ('" + modem + "', '" + nroTambo +"', '" + touridx + "', '" + remito + "', '" + observaciones + "', '" + codigoTambero + "', '" + cantidadTotal + "', '"
                + compA + "', '" + cantidadA + "', '" + compB + "', '" + cantidadB + "', '" + compC + "', '" + cantidadC + "', '" 
                + tempAver + "', '" + tempMin + "', '" + tempMax + "', '" + status + "', '" + litrosTmuestra + "', '" + desaInicial + "', '" + desaFinal + "', '" 
                + constanteE + "', '" + constanteC + "', '" + sampleID + "', '" + fechaHoraInicio + "', '" + fechaHoraFin  + "', '" + tag + "', '" + rx_patente + "');";
              
              //Se inserta fila gpspos con ícono de tambo
              query2 = "";
              //Actualiza uTambos
              query3 = "REPLACE INTO `insoltechv12`.`uTambos` (  `modem`,`tag`, `mapIcon`, `nroCamion`, `nroTambo`, `cantidad`, `patente`) VALUES ('" 
                + modem + "', '" + tag + "', '" + tambo_icon + "', '" + nro_camion + "', '" + remito
                + "', '" + codigoTambero + "', '" + rx_patente + "');";
            }
            else
            {
              AccuracyGlobal = 20;
              query1 = "INSERT INTO `insoltechv12`.`tambo` ( `modem`, `nroTambo`, `touridx`, `remito`, `observaciones`, `codigoTambero`, `cantidadTotal`, `compa`,"
                + " `cantidadA`, `compb`, `cantidadB`, `compc`, `cantidadC`, `tempaver`, `tempmin`, `tempmax`, `status`, `LTM`, `desaInicial`, `desaFinal`,"
                + " `constanteE`, `constanteC`, `SMP`, `FechaHoraInicio`, `FechaHoraFin`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `tag`, `patente`)"
                + "VALUES ('" + modem + "', '" + nroTambo +"', '" + touridx + "', '" + remito + "', '" + observaciones + "', '" + codigoTambero + "', '" + cantidadTotal + "', '"
                + compA + "', '" + cantidadA + "', '" + compB + "', '" + cantidadB + "', '" + compC + "', '" + cantidadC + "', '" 
                + tempAver + "', '" + tempMin + "', '" + tempMax + "', '" + status + "', '" + litrosTmuestra + "', '" + desaInicial + "', '" + desaFinal + "', '" 
                + constanteE + "', '" + constanteC + "', '" + sampleID + "', '"
                + fechaHoraInicio + "', '"+ fechaHoraFin  + "', '"+ GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" + Kilometros + "', '" + tag + "', '" + rx_patente + "');";            
              
                //Se inserta fila gpspos con ícono de tambo
              query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, `mapIcon`, "
                +"`IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" 
                + Kilometros + "', '" + AccuracyGlobal + "', '" + tag + "', '" + tambo_icon + "', 'Tambo', '"+ rx_patente + "');";
              
                //Actualiza uTambos
              query3 = "REPLACE INTO `insoltechv12`.`uTambos` (  `modem`, `GPSlat`, `GPSlng`,`tag`, `mapIcon`, `nroCamion`, `nroTambo`, `cantidad`, `patente`) VALUES ('" 
                + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + tag + "', '" + tambo_icon + "', '" + nro_camion + "', '" + remito
                + "', '" + codigoTambero + "', '" + rx_patente + "');";
            }
          }
          else
          {
            query1 = "INSERT INTO `insoltechv12`.`tambo` ( `modem`, `nroTambo`, `touridx`, `remito`, `observaciones`, `codigoTambero`, `cantidadTotal`, `compa`,"
              + " `cantidadA`, `compb`, `cantidadB`, `compc`, `cantidadC`, `tempaver`, `tempmin`, `tempmax`, `status`, `LTM`, `desaInicial`, `desaFinal`,"
              + " `constanteE`, `constanteC`, `SMP`, `FechaHoraInicio`, `FechaHoraFin`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `gpsTime`, `tag`, `patente`)"
              + "VALUES ('" + modem + "', '" + nroTambo +"', '" + touridx + "', '" + remito + "', '" + observaciones + "', '" + codigoTambero + "', '" + cantidadTotal + "', '"
              + compA + "', '" + cantidadA + "', '" + compB + "', '" + cantidadB + "', '" + compC + "', '" + cantidadC + "', '" 
              + tempAver + "', '" + tempMin + "', '" + tempMax + "', '" + status + "', '" + litrosTmuestra + "', '" + desaInicial + "', '" + desaFinal + "', '" 
              + constanteE + "', '" + constanteC + "', '" + sampleID + "', '"
              + fechaHoraInicio + "', '"+ fechaHoraFin  + "', '"+ GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" + Kilometros + "', '" + gpsFechaHora + "', '" + tag + "', '" + rx_patente + "');";	
                      
            //Se inserta fila gpspos con ícono de tambo
            query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, `gpsTime`, `mapIcon`, "
              +"`IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" 
              + Kilometros + "', '" + AccuracyGlobal + "', '" + tag + "', '" + gpsFechaHora + "', '" + tambo_icon + "', 'Tambo', '" + rx_patente + "');";

            //Actualiza uTambos
            query3 = "REPLACE INTO `insoltechv12`.`uTambos` (  `modem`, `GPSlat`, `GPSlng`,`tag`, `gpsTime`, `mapIcon`, `nroCamion`, `nroTambo`, `cantidad`, `patente`) VALUES ('" 
              + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + tag + "', '" + gpsFechaHora + "', '" + tambo_icon + "', '" + nro_camion + "', '" + remito
              + "', '" + codigoTambero + "', '" + rx_patente + "');";
          }
          try
          {
            con.query(query1, function (err, result, fields) 
            {
              if (err)
              {
                var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
                con.query(queryError, function (err, result, fields) 
                {
                    console.log("Mensaje irreconocido insertado correctamente");
                });
              }
              console.log("Fila 'tambo' insertada correctamente");
            });
  
            if((parseFloat(GPSlat) != 0) && parseFloat(GPSlng) != 0)
            {
              con.query(query2, function (err, result, fields) 
              {
                if (err)
                {
                    var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
                    con.query(queryError, function (err, result, fields) 
                    {
                        console.log("Mensaje irreconocido insertado correctamente");
                    });
                }
                console.log("Fila 'gpspos' insertada correctamente");
              });
            }  
            var query = "SELECT modem, nroCamion FROM camion";
            con.query(query, function (err, result, fields)
            {
              if (err) 
              {
                var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
                con.query(queryError, function (err, result, fields) 
                {
                    console.log("Mensaje irreconocido insertado correctamente");
                });
              }
              for(const element of result)
              {
                if(element.modem == modem)
                {
                  nro_camion = element.nroCamion;
                  break;
                }
              }
              con.query(query3, function (err, result, fields) 
              {
                if (err)
                {
                    var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
                    con.query(queryError, function (err, result, fields) 
                    {
                        console.log("Mensaje irreconocido insertado correctamente");
                    });
                }
                console.log("Fila 'uTambos' insertada correctamente");
              });              
            });
          }
          catch
          {
            var queryError = "INSERT INTO `insoltechv12`.`errores` (`mensaje`) VALUES ('" + msg + "');";
            con.query(queryError, function (err, result, fields) 
            {
                console.log("Mensaje irreconocido insertado correctamente");
            });
          }         
        }
      }  
      if (topic == "cliente2/cip")
      {
        var modem = json_parser['md'];
        var nroCip = json_parser['fld']['nC'];
        var touridx = json_parser['fld']['tid'];
        var nroCamion = json_parser['fld']['uNbr'];
        var tempAver = json_parser['fld']['tAv'];
        var tempMin = json_parser['fld']['tMn']; 
        var tempMax = json_parser['fld']['tMx']; 
        var totalLitros = json_parser['fld']['TL'];
        var fechaHoraInicio = json_parser['fld']['FHI'];
        var fechaHoraFin = json_parser['fld']['FHF'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(modem=="qt-008")
          Kilometros-=10420.2;
        if(globalTag!=tag)
        {
          globalTag = tag;
          if(gpsFechaHora=="2021-01-01 08:00:00")
          { //Se inserta fila cip
            if((parseFloat(GPSlat) == 0) || (parseFloat(GPSlng) == 0))
            {
              query1 = "INSERT INTO `insoltechv12`.`cip` (  `modem`, `nroCip`, `touridx`, `nroCamion`, `tempaver`, `tempmin`, `tempmax`,"
                + " `totalLitros`, `FechaHoraInicio`, `FechaHoraFin`, `tag`, `patente`)"
                +" VALUES ('" + modem + "', '" + nroCip +"', '" + touridx + "', '" + nroCamion + "', '" + tempAver + "', '" + tempMin + "', '"
                + tempMax + "', '" + totalLitros + "', '" + fechaHoraInicio + "', '"+ fechaHoraFin  + "', '" + tag + "', '" + rx_patente + "');";
              
              //Se inserta fila gpspos con ícono de cip
              query2 = "";
            }
            else
            {
              query1 = "INSERT INTO `insoltechv12`.`cip` (  `modem`, `nroCip`, `touridx`, `nroCamion`, `tempaver`, `tempmin`, `tempmax`,"
              + " `totalLitros`, `FechaHoraInicio`, `FechaHoraFin`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `tag`, `patente`)"
              +" VALUES ('" + modem + "', '" + nroCip +"', '" + touridx + "', '" + nroCamion + "', '" + tempAver + "', '" + tempMin + "', '"
              + tempMax + "', '" + totalLitros + "', '" + fechaHoraInicio + "', '"+ fechaHoraFin  + "', '"+ GPSlat +"', '" + GPSlng + "', '"
              + Velocidad + "', '" + Kilometros + "', '" + tag + "', '" + rx_patente + "');";
              
              AccuracyGlobal = 20;
            //Se inserta fila gpspos con ícono de cip
              query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, "
                + "`mapIcon`, `IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad
                + "', '" + Kilometros + "', '" + AccuracyGlobal + "', '" + tag + "', '" + cip_icon + "', 'CIP', '"+ rx_patente + "');";  
            }
          }
          else
          { //Se inserta fila cip
            query1 = "INSERT INTO `insoltechv12`.`cip` (  `modem`, `nroCip`, `touridx`, `nroCamion`, `tempaver`, `tempmin`, `tempmax`,"
              + " `totalLitros`, `FechaHoraInicio`, `FechaHoraFin`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `gpsTime`, `tag`, `patente`)"
              +" VALUES ('" + modem + "', '" + nroCip +"', '" + touridx + "', '" + nroCamion + "', '" + tempAver + "', '" + tempMin + "', '"
              + tempMax + "', '" + totalLitros + "', '" + fechaHoraInicio + "', '"+ fechaHoraFin  + "', '"+ GPSlat +"', '" + GPSlng + "', '"
              + Velocidad + "', '" + Kilometros + "', '" + gpsFechaHora + "', '" + tag + "', '" + rx_patente + "');";		

            //Se inserta fila gpspos con ícono de cip
            query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, `gpsTime`, "
              + "`mapIcon`, `IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad
              + "', '" + Kilometros + "', '" + AccuracyGlobal + "', '" + tag + "', '" + gpsFechaHora + "', '" + cip_icon + "', 'CIP', '"+ rx_patente + "');";  
          }
          con.query(query1, function (err, result, fields) 
          {
            if (err) throw err;
            console.log("Fila 'cip' insertada correctamente");
          });
          
          if((parseFloat(GPSlat) != 0) && (parseFloat(GPSlng) != 0))
          {
            con.query(query2, function (err, result, fields) 
            {
              if (err) throw err;
              console.log("Fila 'gpspos' insertada correctamente");
            });
          }
        }
      }//ok
      if (topic == "cliente2/trasvase")
      {
        var modem = json_parser['md'];
        var nroTrasvase = json_parser['fld']['nTr'];
        var touridx = json_parser['fld']['tid'];
        var sourceComp = json_parser['fld']['sC'];
        var destComp = json_parser['fld']['dC'];
        var trailerNo = json_parser['fld']['trN'];
        var cantidad = json_parser['fld']['cnt'];
        var fechaHora = json_parser['fld']['FH'];    
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(modem=="qt-008")
          Kilometros-=10420.2;
        if(globalTag!=tag)
        {
          globalTag = tag;
          if(gpsFechaHora=="2021-01-01 08:00:00")
          { //Se inserta fila trasvase
            if((parseFloat(GPSlat) == 0) || (parseFloat(GPSlng) == 0))
            {
              query1 = "INSERT INTO `insoltechv12`.`trasvase` (  `modem`, `nroTrasvase`, `touridx`, `Sourcecomp`, `destcomp`, `trailerno`,"
                + " `cantidad`, `FechaHora`, `tag`, `patente`) VALUES ('" + modem + "', '"
                + nroTrasvase + "', '" + touridx + "', '" + sourceComp + "', '" + destComp + "', '" + trailerNo + "', '" + cantidad + "', '" 
                + fechaHora + "', '" + tag + "', '" + rx_patente + "');";
              //Se inserta fila gpspos con ícono de trasvase
              query2 = "";
            }
            else
            {
              query1 = "INSERT INTO `insoltechv12`.`trasvase` (  `modem`, `nroTrasvase`, `touridx`, `Sourcecomp`, `destcomp`, `trailerno`,"
                + " `cantidad`, `FechaHora`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `tag`, `patente`) VALUES ('" + modem + "', '"
                + nroTrasvase + "', '" + touridx + "', '" + sourceComp + "', '" + destComp + "', '" + trailerNo + "', '" + cantidad + "', '" 
                + fechaHora + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" + Kilometros + "', '"
                + tag + "', '" + rx_patente + "');";
                AccuracyGlobal = 20;
              //Se inserta fila gpspos con ícono de trasvase
              query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `Velocidad`, `Kilometros`, `Accuracy`, `GPSlat`, `GPSlng`, `tag`,"
                + " `mapIcon`, `IconName`, `patente`) VALUES ('" + modem + "', '" + Velocidad + "', '" + Kilometros + "', '" 
                + AccuracyGlobal + "', '" + GPSlat + "', '" + GPSlng + "', '" + tag + "', '" + trasvase_icon + "', 'Trasvase', '" + rx_patente + "');";
            }
          }
          else
          { //Se inserta fila trasvase
            query1 = "INSERT INTO `insoltechv12`.`trasvase` (  `modem`, `nroTrasvase`, `touridx`, `Sourcecomp`, `destcomp`, `trailerno`,"
              + " `cantidad`, `FechaHora`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `gpsTime`, `tag`, `patente`) VALUES ('" + modem + "', '"
              + nroTrasvase + "', '" + touridx + "', '" + sourceComp + "', '" + destComp + "', '" + trailerNo + "', '" + cantidad + "', '" 
              + fechaHora + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" + Kilometros + "', '"
              + gpsFechaHora + "', '" + tag + "', '" + rx_patente + "');";
              //Se inserta fila gpspos con ícono de trasvase
            query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`, `gpsTime`,"
              + " `mapIcon`, `IconName`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad
              + "', '" + Kilometros + "', '" + AccuracyGlobal + "', '" + tag + "', '" + gpsFechaHora + "', '" + trasvase_icon + "', 'Trasvase', '" + rx_patente + "');";
          }
            
          con.query(query1, function (err, result, fields) 
          {
            if (err) throw err;
            console.log("Fila 'trasvase' insertada correctamente");
          });

          if((parseFloat(GPSlat) != 0) && (parseFloat(GPSlng) != 0))
          {  
            con.query(query2, function (err, result, fields) 
            {
              if (err) throw err;
              console.log("Fila 'gpspos' insertada correctamente");
            });
          }
        }
      }
      if (topic == "cliente2/gps") 
      {
        var modem = json_parser['md'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];;
        var Velocidad = json_parser['gps']['spd'];
        var Kilometros = json_parser['gps']['cd'];
        var Accuracy = json_parser['gps']['acc'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var gprs = json_parser['gprs'];
        var tag = json_parser['tag'];
        var nombre_archivo;
        var color;
        var nro_camion;
        if(modem=="qt-008")
          Kilometros-=10420.2;
        AccuracyGlobal = Accuracy;
        var query0 = "SELECT modem, nroCamion, Icono, Color FROM camion";
        if(globalTag!=tag)
        {
          globalTag = tag;
          con.query(query0, function (err, result, fields)
          {
            if (err) throw err;
            for(const element of result)
            {
              if(element.modem == modem)
              {
                nro_camion = element.nroCamion;
                nombre_archivo = element.Icono;
                color = element.Color;
                break;
              }
            }
            console.log(modem, nro_camion, nombre_archivo, color);
            var query2 = "INSERT INTO `insoltechv12`.`gpspos` (  `modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `Accuracy`, `tag`,"
              + " `gpsTime`, `mapIcon`, `nroCamion`, `gprs`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '"
              + Kilometros + "', '" + Accuracy + "', '" + tag + "', '" + gpsFechaHora + "', '" + nombre_archivo + "', '" + nro_camion + "', '" + gprs + "', '" + rx_patente + "');";
            con.query(query2, function (err, result, fields) 
            {
              if (err) throw err;
              console.log("Fila 'gpspos' insertada correctamente");
              var query = "REPLACE INTO `insoltechv12`.`uPos` (`modem`, `GPSlat`, `GPSlng`, `Velocidad`, `Kilometros`, `tag`, `gpsTime`, "
                + "`mapIcon`, `nroCamion`, `patente`) VALUES ('" + modem + "', '" + GPSlat +"', '" + GPSlng + "', '" + Velocidad + "', '" + Kilometros
                + "', '" + tag + "', '" + gpsFechaHora + "', '" + color + "', '" + nro_camion + "', '" + rx_patente + "');";
              con.query(query, function (err, result, fields) 
              {
                if (err) throw err;
                console.log("Fila 'uPos' insertada correctamente");
              });
            });
          });
        }
      }
      if (topic == "cliente2/fails")
      {
        var modem = json_parser['md'];
        var sd = json_parser['fld']['SD'];
        var q = json_parser['fld']['Q'];
        var gps = json_parser['fld']['GPS'];
        var nro_resets = json_parser['fld']['RESETS'];   
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        if(sd=="1")
          sd="true";
        else
          sd="false";
        if(q=="1")
          q="true";
        else
          q="false";
        if(gps=="1")
          gps="true";
        else
          gps="false";

        var query = "INSERT `insoltechv12`.`alertas` SET `modem` = '" + modem + "', `SD` = '" + sd
        + "', `Q` = '" + q + "', `GPS` = '" + gps + "', `resets` = '" + nro_resets + "', `GPSlat` = '"
        + GPSlat + "', `GPSlng` = '" + GPSlng + "', `gpsTime` = '" + gpsFechaHora + "', `tag` = '"
        + tag + "';";
        
        if(gpsFechaHora=="2021-01-01 08:00:00")
        {
          query = "INSERT `insoltechv12`.`alertas` SET `modem` = '" + modem + "', `SD` = '" + sd
          + "', `Q` = '" + q + "', `GPS` = '" + gps + "', `resets` = '" + nro_resets + "', `GPSlat` = '"
          + GPSlat + "', `GPSlng` = '" + GPSlng + "', `tag` = '"
          + tag + "';";
        }          
        
        console.log(query);    
        con.query(query, function (err, result, fields) 
        {
          if (err) throw err;
          console.log("Fila 'falla' insertada correctamente");
        });
      }
      if (topic == "cliente2/parametros")
      {
        var modem = json_parser['md'];
        var uNbr = json_parser['fld']['uNbr'];
        var trailN = json_parser['fld']['trailN'];
        var nCta = json_parser['fld']['nCta'];
        var nCis = json_parser['fld']['nCis'];
        var cC1 = json_parser['fld']['cC1'];  
        var cC2 = json_parser['fld']['cC2']; 
        var cC3 = json_parser['fld']['cC3'];
        var cC4 = json_parser['fld']['cC4'];
        var cC5 = json_parser['fld']['cC5'];
        var PT100 = json_parser['fld']['PT100'];
        var zElec = json_parser['fld']['zElec'];
        var ctElec = json_parser['fld']['ctElec'];
        var ctCab = json_parser['fld']['ctCab'];
        var qMax = json_parser['fld']['qMax'];
        var qMin = json_parser['fld']['qMin'];
        var onT = json_parser['fld']['onT'];
        var offT = json_parser['fld']['offT'];
        var Linit = json_parser['fld']['Linit'];
        var pulsos = json_parser['fld']['pulsos'];
        var TMcipT = json_parser['fld']['TMcipT'];
        var atrasT = json_parser['fld']['atrasT'];
        var minQ = json_parser['fld']['minQ'];
        var Tbop = json_parser['fld']['Tbop'];
        var TBbcl = json_parser['fld']['Tbcl'];
        var Tcop = json_parser['fld']['Tcop'];
        var Tccl = json_parser['fld']['Tccl'];
        var Tdbo = json_parser['fld']['Tdbo'];
        var Tdda = json_parser['fld']['Tdda'];
        var NivA = json_parser['fld']['NivA'];
        var NivB = json_parser['fld']['NivB'];
        var NivB1 = json_parser['fld']['NivB1'];
        var DAcipT = json_parser['fld']['DAcipT'];
        var cMn = json_parser['fld']['cMn'];
        var cMx = json_parser['fld']['cMx'];
        var Loff = json_parser['fld']['Loff'];
        var tLMx = json_parser['fld']['tLMx'];
        var minLT = json_parser['fld']['minLT'];
        var q0start = json_parser['fld']['q0start'];
        var q0end = json_parser['fld']['q0end'];
        var q0Th = json_parser['fld']['q0Th'];
        var cipMxT = json_parser['fld']['cipMxT'];
        var serialq_nr = json_parser['fld']['sQNR'];
        var q_to = json_parser['fld']['sQTO'];
        var dCgps = json_parser['fld']['dCgps'];      
        var gps_store_period = json_parser['fld']['dTgps'];
        var min_delta = json_parser['fld']['dDgps'];
        var offTgps = json_parser['fld']['offTgps'];
        var backoff = json_parser['fld']['backoff'];
        var gpscol = json_parser['fld']['gpscol'];
        var gpsstream = json_parser['fld']['gpsstream'];
        var netto = json_parser['fld']['netto'];
        var minpow = json_parser['fld']['minpow'];
        var GPSlat = json_parser['gps']['lat'];
        var GPSlng = json_parser['gps']['lng'];
        var gpsFechaHora = json_parser['gps']['gTm'];
        var tag = json_parser['tag'];
        var query_param;
        if(globalTag!=tag)
        {
          globalTag = tag;
          if(gpsFechaHora=="2021-01-01 08:00:00")
          {
            query_param = "INSERT INTO `insoltechv12`.`param_report` ( `modem`, `uNbr`, `trailN`," +
              " `nCta`, `nCis`, `cC1`, `cC2`, `cC3`, `cC4`, `cC5`, `PT100`, `zElec`, `ctElec`," +
              " `ctCab`, `qMax`, `qMin`, `onT`, `offT`, `Linit`, `pulsos`, `TMcipT`, `atrasT`," +
              " `minQ`, `Tbop`, `Tbcl`, `Tcop`, `Tccl`, `Tdbo`, `Tdda`, `NivA`, `NivB`, `NivB1`," +
              " `DAcipT`, `cMn`, `cMx`, `Loff`, `tLMx`, `minLT`, `q0start`, `q0end`, `q0Th`, `cipMxT`," +
              " `GPSlat`, `GPSlng`, `gpsTime`, `tag`, `sQNR`, `sQTO`, `dCgps`, `dTgps`, `dDgps`, `offTgps`," +
              " `backoff`, `gpscol`, `gpsstream`, `netto`, `minpow`, `patente`)" + 
              "VALUES ('" + modem + "', '" + uNbr +"', '" + trailN + "', '" + nCta + "', '" + nCis + "', '" + 
              cC1 + "', '" + cC2 + "', '" + cC3 + "', '" + cC4 + "', '" + cC5 + "', '" + PT100 + "', '" + 
              zElec + "', '" + ctElec + "', '" + ctCab + "', '" + qMax + "', '" + qMin + "', '" + onT + "', '" + 
              offT + "', '" + Linit + "', '" + pulsos + "', '" + TMcipT + "', '" + atrasT + "', '" + minQ + "', '" + 
              Tbop  + "', '"+ TBbcl +"', '" + Tcop + "', '" + Tccl + "', '" + Tdbo +"', '" + Tdda + "', '" + 
              NivA + "', '" + NivB + "', '" + NivB1 + "', '" + DAcipT + "', '" + cMn + "', '" + cMx + "', '" + 
              Loff + "', '" + tLMx + "', '" + minLT + "', '" + q0start + "', '" + q0end + "', '" + q0Th + "', '" + 
              cipMxT + "', '" + GPSlat + "', '" + GPSlng + "', CURRENT_TIMESTAMP, '" + tag + "', '" + serialq_nr + "', '" + q_to + "', '" +
              dCgps + "', '" + gps_store_period + "', '" + min_delta + "', '" + offTgps + "', '" + backoff + "', '" + 
              gpscol + "', '" + gpsstream + "', '" + netto+ "', '" + minpow + "', '" + rx_patente + "');";
          }
          else
          {
            query_param = "INSERT INTO `insoltechv12`.`param_report` ( `modem`, `uNbr`, `trailN`," +
              " `nCta`, `nCis`, `cC1`, `cC2`, `cC3`, `cC4`, `cC5`, `PT100`, `zElec`, `ctElec`," +
              " `ctCab`, `qMax`, `qMin`, `onT`, `offT`, `Linit`, `pulsos`, `TMcipT`, `atrasT`," +
              " `minQ`, `Tbop`, `Tbcl`, `Tcop`, `Tccl`, `Tdbo`, `Tdda`, `NivA`, `NivB`, `NivB1`," +
              " `DAcipT`, `cMn`, `cMx`, `Loff`, `tLMx`, `minLT`, `q0start`, `q0end`, `q0Th`, `cipMxT`," +
              " `GPSlat`, `GPSlng`, `gpsTime`, `tag`, `sQNR`, `sQTO`, `dCgps`, `dTgps`, `dDgps`, `offTgps`," +
              " `backoff`, `gpscol`, `gpsstream`, `netto`, `minpow`, `patente`)" +  
              "VALUES ('" + modem + "', '" + uNbr +"', '" + trailN + "', '" + nCta + "', '" + nCis + "', '" + 
              cC1 + "', '" + cC2 + "', '" + cC3 + "', '" + cC4 + "', '" + cC5 + "', '" + PT100 + "', '" + 
              zElec + "', '" + ctElec + "', '" + ctCab + "', '" + qMax + "', '" + qMin + "', '" + onT + "', '" + 
              offT + "', '" + Linit + "', '" + pulsos + "', '" + TMcipT + "', '" + atrasT + "', '" + minQ + "', '" + 
              Tbop  + "', '"+ TBbcl +"', '" + Tcop + "', '" + Tccl + "', '" + Tdbo +"', '" + Tdda + "', '" + 
              NivA + "', '" + NivB + "', '" + NivB1 + "', '" + DAcipT + "', '" + cMn + "', '" + cMx + "', '" + 
              Loff + "', '" + tLMx + "', '" + minLT + "', '" + q0start + "', '" + q0end + "', '" + q0Th + "', '" + 
              cipMxT + "', '" + GPSlat + "', '" + GPSlng + "', '" + gpsFechaHora + "', '" + tag + "', '" + serialq_nr + "', '" + q_to + "', '" +
              dCgps + "', '" + gps_store_period + "', '" + min_delta + "', '" + offTgps + "', '" + backoff + "', '" + 
              gpscol + "', '" + gpsstream + "', '" + netto+ "', '" + minpow + "', '" + rx_patente + "');";	
          }
          con.query(query_param, function (err, result, fields) 
          {
            if (err) throw err;
            console.log("Fila 'parametros' insertada correctamente");
          });
        }
      }
    }
  }
});

//nos conectamos
con.connect(function(err){
  if (err) throw err;

  //una vez conectados, podemos hacer consultas.
  console.log("Conexión a MYSQL exitosa!!!")
//When a new reconfig entry is adde to param_reconfig, a new entry is made in time_reconfig
//marking the beggining of the reconfig attempt
  var query0 = "DROP TRIGGER IF EXISTS param_timestamp_update;";
  con.query(query0, function (err, result, fields) {
    var query1 ="CREATE TRIGGER param_timestamp_update " +
    "AFTER INSERT ON param_reconfig FOR EACH ROW " +
    "INSERT reconfig_time(`update`, `modem`, `param_id`) VALUES (CURRENT_TIMESTAMP, ( SELECT `modem` FROM param_reconfig WHERE `param_id` = ( SELECT MAX(`param_id`) FROM param_reconfig )), ( SELECT MAX(`param_id`) FROM param_reconfig));";
      con.query(query1, function(err, result, fields) {
        if (err) throw err;
        console.log("Trigger param_timestamp_update created");
      }); 
  });

  var query2 = "DROP TRIGGER IF EXISTS new_truck_alert_sign_up;";
  con.query(query2, function (err, result, fields) {
  });
});

//para mantener la sesión con mysql abierta
setInterval(function () {
  var query ='SELECT 1 + 1 as result';
  con.query(query, function (err, result, fields) {
    if (err) throw err;
  });
}, 5000);

//periodically query reconfig_time to catch the last param reconfig order
//every order will be published only once, it always fetches max param_id for building reconfig msg
setInterval(function () {//When a new entry is made in param_reconfig, a new entry is made in reconfig_time and we catch this entry
  var first = false;//we catch always the last entry
  var reconfig_msg_aux2;
  var query ='SELECT * FROM reconfig_time WHERE `acknowledge` IS NULL AND `nacknowledge` IS NULL AND `update` = (SELECT MAX(`update`) FROM reconfig_time)';
  try{
    con.query(query, function (err, result, fields) {
      if (err)
      {
        //console.log(err);
      }
      else
      {
        try
        {
          if(update_sent!=String(result[0]['update']))
          {
            update_sent=String(result[0]['update']);
            var reconfig_msg;
            var reconfig_msg_aux;
            var topic = "cliente2/reconfig/" + String(result[0]['modem']);
            var query ='SELECT * FROM param_reconfig WHERE `param_id` = ( SELECT MAX(`param_id`) FROM param_reconfig )';
            try
            {
              con.query(query, function(err, result, fields) {
                if (err)
                {
                  //console.log(err);
                }
                try
                {
                  reconfig_msg = '{"md":"' + String(result[0]['modem']) + '", "msg":"PARAM", "IOT": { ';
                  for (const item in result[0])
                  {
                    if(String(item)=="param_id")
                    {
                      reconfig_msg_aux2 = reconfig_msg_aux.replaceAt(reconfig_msg_aux.length-1, '}');     
                      reconfig_msg_aux2 = reconfig_msg_aux2 + ', "' + String(item) + '":"' + String(result[0][String(item)]) + '" }';
                    }
                    if(String(item)=="unbr")
                    {
                      reconfig_msg_aux = reconfig_msg.replaceAt(reconfig_msg.length-1, '}');
                      console.log(reconfig_msg_aux);
                      console.log(reconfig_msg);
                      reconfig_msg_aux = reconfig_msg_aux + ', "FLOW": { ';
                      first = true;
                    }
                    if(result[0][String(item)] != null && result[0][String(item)] !== "" && String(item) != 'modem')
                    {
                      if(!first)
                      {
                        reconfig_msg = reconfig_msg + '"' + String(item) + '":"' + String(result[0][String(item)]) + '",';
                      }
                      else
                      {
                        reconfig_msg_aux = reconfig_msg_aux + '"' + String(item) + '":"' + String(result[0][String(item)]) + '",';
                      }
                    }
                  }   
                }
                catch(err)
                {
                  
                }           
                console.log(reconfig_msg_aux2);
                mqtt_client.publish(topic, reconfig_msg_aux2, {retain: true});
                console.log("Msg sent");
              });
            }
            catch(err)
            {
             
            }
          }
        }
        catch(err)
        {
          
        }
      }
    });
  }
  catch(err)
  {
    //console.log(err);
  }

  var query ='SELECT * FROM reconfig_time WHERE `solicitud` IS NOT NULL AND `time_id`=(SELECT MAX(`time_id`) FROM reconfig_time)';
  try{
    con.query(query, function (err, result, fields) {
      if (err)
      {
        //console.log(err);
      }
      else
      {
        try
        {
          //console.log("cliente2/result");
          console.log(result[0]['solicitud']);
          if(last_sol != String(result[0]['solicitud']))
          {
            console.log("sol Up");
            var sol_msg;
            var sol_topic;
            last_sol = String(result[0]['solicitud']);
            sol_topic = "cliente2/report/" + result[0]["modem"];
            sol_msg = '{"msg":"REPORT"}';
            console.log(sol_topic)
            console.log(sol_msg);
            mqtt_client.publish(sol_topic, sol_msg, {retain: true});
          }
          else
          {
            
          }
        }
        catch(err)
        {
          //console.log(err);
        }
      }
    });
  }
  catch(err)
  {
    //console.log(err);
  }
}, 1000);

String.prototype.replaceAt = function(index, replacement) {
  return this.substr(0, index) + replacement + this.substr(index + replacement.length);
}
