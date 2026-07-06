import java.io.IOException;
import java.net.DatagramPacket;
import java.util.logging.Level;
import java.nio.*;
import java.nio.charset.StandardCharsets;



public class SW extends ARQAbst 
{

    public SW(Socket socket)
    {
        super(socket);
    }

    public SW(Socket socket, int sessionID)
    {
        super(socket, sessionID);
    }

    @java.lang.Override
    public void closeConnection() {
    
    CurrentPackage = 0;
    Serverstate=0;      //1 nach SP 2 nach letzdem SP 0 wenn Conection closed bzw. Server Start
    server_sessionnr=0;
    datasize_check=1024;

    }


    @java.lang.Override
    public boolean data_req(byte[] hlData, int hlSize, boolean lastTransmission) 
    {
        byte[] data = generateDataPacket(hlData, hlSize);

        //in data befinden sich jetzt Paketinformationen 

        ByteBuffer data_buff = ByteBuffer.wrap(data);
        int dataLength = data_buff.remaining();
        data_buff.rewind();
        short sessionnum=data_buff.getShort();
        byte packetnum=data_buff.get();

        String marker="";
        if(dataLength<=8) //letztes DP mit kleiner als 8 Byte
        {
            marker = new String(new byte[5], StandardCharsets.UTF_8);       
            marker = "Daten";
        }
        else
        {
        byte[] marker_byte = new byte[5];
        data_buff.get(marker_byte);
        marker=new String(marker_byte, StandardCharsets.UTF_8);
        }
        //logger.log(Level.FINEST, "Client-SW: " + "Marker-Byte"+ marker);
        int max_timeouts=10;
        for(int cur_timeout=0;cur_timeout<=max_timeouts;)
        {
         try 
         {
            if(marker.equals("Start")) //SP
            {
             logger.log(Level.FINEST, "Client-SW: " + "SP send, Waiting for ACK| sessionnum: "+ sessionnum);
             socket.sendPacket(data);
             if(waitForAck(packetnum,sessionnum,false)==false)
             {cur_timeout++;}
             else {return true;}
            }
            else if((!marker.equals("Start")) && (!marker.equals("Final")) && lastTransmission==false) //DP
            {
           // logger.log(Level.FINEST, "Client-SW: " + "DP send, Waiting for ACK| sessionnum: "+ sessionnum);
             socket.sendPacket(data);
             if(waitForAck(packetnum,sessionnum,false)==false)
             {cur_timeout++;}
             else {return true;}
            }
            else if(marker.equals("Final")) //FP Client
            {

             logger.log(Level.FINEST, "Client-SW: " + "sending FP ");
             
             int FP_crc32_sum=data_buff.getInt();

             ByteBuffer mp_FP_buff=ByteBuffer.allocate(7);
             mp_FP_buff.putShort(sessionnum);
             mp_FP_buff.put(packetnum); //packetNur rein
             mp_FP_buff.putInt(FP_crc32_sum);

             DatagramPacket empty_FP = new DatagramPacket(mp_FP_buff.array(), mp_FP_buff.limit());
             byte[] result = empty_FP.getData();
             
             socket.sendPacket(result);

             if(waitForAck(packetnum,sessionnum,true)==false) 
             {cur_timeout++;}

             else 
             {return true;}

            } //--------------------------FACK---------------------------------------------------------------
            else if((!marker.equals("Start")) && (!marker.equals("Final")) && lastTransmission==true) //FAck
            {
                logger.log(Level.FINEST, "Server-SW: " + "sending FAck");
                socket.sendPacket(data);
                return true;
            }
            else
            {
            //logger.log(Level.FINEST, "Client-SW: kein gültiges Packet (not a valid packet)");
            throw new IOException("Invalid packet received");
            }
         }
         catch (IOException e) 
            {
                e.printStackTrace(); // Handle IOException appropriately
                return false;
            }
        }
        logger.log(Level.FINEST,"max timeouts reached closeing session");
        System.exit(1);
        return false;
    }
        
        

    @java.lang.Override
    protected boolean waitForAck(byte packetNr,short sessionnum,boolean last) 
    {
        socket.setTimeout(1000); //timeout in ms
            try 
            {
            DatagramPacket ackPacket;
            ackPacket  = socket.receivePacket();

            byte[] Ack_data= ackPacket.getData();
            ByteBuffer Ack_buff = ByteBuffer.wrap(Ack_data);
            short Ack_sessionnum=Ack_buff.getShort();
            byte Ack_packetNR=Ack_buff.get();
            int int_pack=(Ack_packetNR & 0xFF)%256;
            byte Ack_Fenstergröße=Ack_buff.get();   //bei SW erstmal nicht relevant 
            //logger.log(Level.FINEST, "WfA: sess"+Ack_sessionnum+ " | packnr: "+int_pack);


         if(Ack_sessionnum!=sessionnum)
         {
          logger.log(Level.FINEST, "Client-SW: Ungültige Sessionnum beim ACK:"+Ack_sessionnum);
          logger.log(Level.FINEST, "Client-SW: Paket wird verworfen");
          return false;
         }
         else if(Ack_packetNR!=packetNr)
         {
          logger.log(Level.FINEST, "Client-SW: Ungültige PacketNR beim ACK:"+Ack_packetNR);
          logger.log(Level.FINEST, "Client-SW: Paket wird verworfen");
          return false;   
         }
         else if(Ack_sessionnum==sessionnum && Ack_packetNR==packetNr)
         {
          //logger.log(Level.FINEST, "Client-SW: ACK richtig");
          if(last!=true)
          {
            return true;  //nichts  tun SP/DP   
          }
          else if(last ==true)
          {        
            //logger.log(Level.FINEST, "Client-SW: FAck received");
            try
            {
            int Ack_crc32sum=Ack_buff.getInt();
            logger.log(Level.FINEST, "Client-SW: FAck Crc32sum: "+Ack_crc32sum);
            return true;
            }
            catch(BufferUnderflowException e)
            {
                throw new RuntimeException("Keine crc32-sum im FAck!");
            }
          }
          else
          {
           logger.log(Level.FINEST, "Something went wrong while receiving FACK");
           return false;
          }
         }
         else
         {
          logger.log(Level.FINEST, "Something went wrong while receiving ACK");
          return false;
         }
        } catch (TimeoutException e) 
        {
            // Handle timeout exception
            //logger.log(Level.FINEST, "Timeout+1");
            return false;
        }
    
}
        
        

    //Alles  in data_req

    @java.lang.Override
    protected int getPacketNr(DatagramPacket packet) {
        return 0;
    }

    @java.lang.Override
    protected void getAckData(DatagramPacket packet) {

    }

    @java.lang.Override
    protected int getSessionID(DatagramPacket packet) {
        return 0;
    }

    @java.lang.Override
    protected byte[] generateDataPacket(byte[] sendData, int dataSize) {
        return sendData;
    }

    //************************Server**********************************//

    private int CurrentPackage = 0;
    private int Serverstate=0;      //1 nach SP 2 nach letzdem SP 0 wenn Conection closed bzw. Server Start
    private int server_sessionnr=0;
    private int datasize_check=1024; //sinnvollen Startwert nehmen || Workaround für lokalen Server nicht gut gelöst!

    @java.lang.Override
    public byte[] data_ind_req(int... values) throws TimeoutException 
    {
        DatagramPacket rec_dataPacket;        
        boolean end_arr=false;
        socket.setTimeout(30000); //timeout in ms hier 300 Sekunden=5min
    try{
        // logger.log(Level.CONFIG,"Server-SW: " + "waiting for packet");
        rec_dataPacket = socket.receivePacket();
        byte[] reqdata=rec_dataPacket.getData();   
        int dataPacket_len= rec_dataPacket.getLength();
        ByteBuffer data_buff = ByteBuffer.wrap(reqdata,0,dataPacket_len); 
        byte[] proc_data= new byte[dataPacket_len];
        data_buff.get(proc_data);
        data_buff.rewind();

        DatagramPacket proc_dataPacket = new DatagramPacket(proc_data,dataPacket_len);
        //Paket mit richtiger Paketlänge

        short rec_sessionnum=data_buff.getShort();
        byte byte_packetnum=data_buff.get();
        int int_packetnum=(byte_packetnum & 0xFF)%256;

        if  (checkStart(proc_dataPacket)==true && Serverstate==0) //SP
            { 
             if(CurrentPackage==0)
             {   
             logger.log(Level.CONFIG,"Server-SW: " + "Start Packet received | Sessionnr: " + rec_sessionnum);
             socket.sendPacket(generateAckPacket(byte_packetnum, rec_sessionnum));
             logger.log(Level.CONFIG,"Server-SW: " + "ACK" + int_packetnum + "| "+ rec_sessionnum +" send");
            
             Serverstate=1;
             server_sessionnr=rec_sessionnum;
             CurrentPackage=(CurrentPackage+1)%256;
             return proc_dataPacket.getData();
             }
             else 
             {
                socket.sendPacket(generateAckPacket(byte_packetnum, rec_sessionnum));
                String ackString = "Ack";
                byte[] ackBytes = ackString.getBytes(StandardCharsets.UTF_8);
                return ackBytes;
             }
            }
    
        else if(checkStart(proc_dataPacket)==false && Serverstate==1) //DP
            {
                
                logger.log(Level.CONFIG, "Server-SW: Datapacketnr: "+ int_packetnum +" | "+ rec_sessionnum +" rec");
                if(int_packetnum==CurrentPackage && server_sessionnr==rec_sessionnum)
                {
                    socket.sendPacket(generateAckPacket(byte_packetnum, rec_sessionnum));
                    logger.log(Level.CONFIG,"Server-SW: " + "ACK" + int_packetnum +"| "+ rec_sessionnum +" send");
                    CurrentPackage=(CurrentPackage+1)%256;
                    if(dataPacket_len>=datasize_check)
                    {
                        datasize_check=dataPacket_len;
                    }
                    else
                    {
                      //  logger.log(Level.CONFIG,"Server-SW: got last DP");
                        CurrentPackage=(CurrentPackage+1)%256;
                        Serverstate=2;

                    }
                    return proc_dataPacket.getData();
                }
                else if(int_packetnum!=CurrentPackage && server_sessionnr==rec_sessionnum)
                {
                    logger.log(Level.CONFIG,"Server-SW: DPacketNr: "+CurrentPackage+" expected sending new Ack");
                    socket.sendPacket(generateAckPacket(byte_packetnum, rec_sessionnum));
                    String ackString = "Ack";
                    byte[] ackBytes = ackString.getBytes(StandardCharsets.UTF_8);
                    return ackBytes;
                }
                else
                {
                 logger.log(Level.CONFIG,"Server-SW: unkown sessionnr");   
                }
                
            }
        else if(checkStart(proc_dataPacket)==false && Serverstate==2) //FP
            {
                if(end_arr==false)
                {
                    logger.log(Level.CONFIG, "Server-SW:"+ " FP received");
                    end_arr=true;
                    CurrentPackage=0;
                    return proc_dataPacket.getData(); 
                }
                else
                {
                    //leeres  fp erstellen
                    ByteBuffer mp_FP_buff=ByteBuffer.allocate(4);
                    mp_FP_buff.putShort(rec_sessionnum);
                    mp_FP_buff.put((byte) int_packetnum);
                    mp_FP_buff.put((byte) 0);

                    DatagramPacket empty_FP = new DatagramPacket(mp_FP_buff.array(), mp_FP_buff.limit());
                    byte[] result = empty_FP.getData();
                    return result;
                }
            }
            return null; 

        }
        catch (TimeoutException e)
        {
        logger.log(Level.CONFIG, "Data packet receive timed out");
        closeConnection();
        throw new TimeoutException("Server-SW: receive time out 120 Sekunden | Close Connection");
        }  
    }

//alles außer check in data_in_req
    @java.lang.Override 
    byte[] generateAckPacket(byte packetNr,short  Sessionnr) 
    {
        byte[] Ack_data= new byte[4];
        byte Fenstergröße=1;        //Wird bei SW nicht  näher betrachtet
        ByteBuffer Ack_buff = ByteBuffer.wrap(Ack_data);
        Ack_buff.putShort(Sessionnr);
        Ack_buff.put(packetNr);
        Ack_buff.put(Fenstergröße);
        return(Ack_data);
    }

     @java.lang.Override
    void sendAck(int nr) 
    {
        //nicht benötigt 
    } 



    @java.lang.Override
    boolean checkStart(DatagramPacket packet) //checkt start und  final
    {
        byte[] signdata=packet.getData();
        ByteBuffer sign_buff = ByteBuffer.wrap(signdata);
        int packetlen=packet.getLength();
        if (packetlen<=8)
        {
            return false;
        }
        else
        {
            sign_buff.position(3);
            byte[] signature=new byte[5];
            sign_buff.get(signature);
            String marker= new  String(signature,StandardCharsets.UTF_8);
            // logger.log(Level.CONFIG, "marker:"+marker);
            if(marker.equals("Start")) {return true;}
            else{return false;}
        }
     
    }
    
}