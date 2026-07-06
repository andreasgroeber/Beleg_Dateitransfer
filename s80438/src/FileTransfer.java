import java.io.IOException;
import java.util.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;




public class FileTransfer implements FT
{

    ARQ myARQ;    //siehe  ARQ class
    Logger logger;

    private static String fName =  "init_Null";
    private static int Fenstergröße=0;



    /**Constructor*************************************************/

    public FileTransfer(Socket socket, String dir) 
    {
        myARQ = new SW(socket);
        logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.log(Level.CONFIG , "Server-FT: " + dir );
    }

    public FileTransfer(String host, Socket socket, String fileName, String arq) 
    {
        logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        myARQ = new SW(socket);
        logger.log(Level.CONFIG, "FileTransfer: " + arq + " " + fileName + " " + host); 
        fName=fileName;
        Fenstergröße=Integer.parseInt(arq);

        
    }

    //************  *Client********************************//


    @Override
    public boolean file_req() throws IOException
    {   
        long Totaltime=0;
        /*--------------------file einlesen und name anpassen----------------------------------------------*/

        byte[] fNameBytes= fName.getBytes(StandardCharsets.UTF_8);
        int len =  fNameBytes.length;
        long fNamelen = len & 0xffffffffL; // konvertierung int zu unsigned int
        String fNameUTF = new String(fNameBytes,StandardCharsets.UTF_8);

        //https://stackoverflow.com/questions/9578639/best-way-to-convert-a-signed-integer-to-an-unsigned-long

        /*-------------------------------------------------------------------------------------*/
        /*--------------------CRC32------------------------------------------------------------*/
        CRC32 crc32 = new CRC32();
        int crc32sum=0; //im Endpaket verwendet 4 Byte!
        byte[] fbuff = null;
        long fileSize = 0;
        long fileSizeU = 0; //unsigned

         try
         {
            Path fpath  =    Paths.get(fName);
            fbuff        =   Files.readAllBytes(fpath);
            fileSize    =    fbuff.length;
         //   System.out.println("filesize: "+ fileSize);
            fileSizeU   =    fileSize & 0xffffffffL;    //Bitmaske --> unteren 32 bit werden entfernt --> Zahl immer positiv
         //   System.out.println("filesizeU: "+ fileSize);


            crc32.reset();          
            crc32.update(fbuff);
            crc32sum = (int) crc32.getValue();

         }
         catch (IOException e)
         {
            logger.log(Level.CONFIG, "Client-FT: " + "Error reading file(CRC32):"+ e);
            System.exit(-1);
         }
         /*------------------------------------------------------------------------------------- */
         /*---------------------------------------- Startpaket bauen---------------------------------------------------------------- */
         /*1.CRC Berechnen------------------------------------------------------------ */
        
         short sessionnum = (short) new Random().nextInt(Short.MAX_VALUE+1); //2Byte
         System.out.println("sessionnumber: " + sessionnum);

         //diennt für CRC32 Berechnung
         int SP_tmp_size= 5+8+2+(int)fNamelen;
         ByteBuffer SP_tmp = ByteBuffer.allocate(SP_tmp_size);

         SP_tmp.put((byte)'S').put((byte)'t').put((byte)'a').put((byte)'r').put((byte)'t'); //+5
         SP_tmp.putLong(fileSizeU);//+8
         SP_tmp.putShort((short) fNamelen); //+2
         SP_tmp.put(fNameUTF.getBytes()); //Variabel
         SP_tmp.rewind();


         crc32.reset();             //schon verwendet nicht auskommentieren!
         crc32.update(SP_tmp);
    
         int SP_crc32_check= (int) crc32.getValue(); //4 Byte crc32 
         //logger.log(Level.CONFIG, "Client-FT: " + "SP_tmp_size: " + SP_tmp_size); 
         //logger.log(Level.CONFIG, "Client-FT: " + "crc32_SP: " + SP_crc32_check); 

         /*2. Paket zusammenbauen--------------------------------------------------- */
        
         int SP_size=2+1+5+8+2+(int)fNamelen+4; 
         ByteBuffer SP_buff = ByteBuffer.allocate(SP_size);

         SP_buff.putShort(sessionnum).put((byte)0);//2+1
         SP_buff.put((byte)'S').put((byte)'t').put((byte)'a').put((byte)'r').put((byte)'t'); //+5
         SP_buff.putLong(fileSizeU);//+8
         SP_buff.putShort((short) fNamelen); //+2
         SP_buff.put(fNameUTF.getBytes()); //Variabel
         SP_buff.putInt(SP_crc32_check); //+4
         SP_buff.rewind();


         byte[] SP_arr= new byte[SP_buff.remaining()];
         SP_buff.get(SP_arr);
         /*3.  Paket senden---------------------------------------------------------*/
         logger.log(Level.CONFIG, "Client-FT: " + "sending: SP_send, Sessionnr: " + sessionnum); 
         myARQ.data_req(SP_arr,SP_size,false);
        //writeByteArrayToFile(SP_arr, "testStartpacket_sender.bin"); 
        
        /*----------------------------------------- Daten-Paket--------------------------------------------------------------------- */
        /* 1. Pakete Bauen  1024 Byte datenanteil---------------------------------------*/
        int Datasize=1024;
        int Total_datapackets = (int) (Math.ceil((float) fileSizeU / Datasize)) ;
        logger.log(Level.CONFIG, "totalDPackets: " + Total_datapackets); 
        int Datasize_last=(int) fileSize-(Datasize*(Total_datapackets-1));

        byte[] DP_Arr=null;
        ByteBuffer DP_buff=null;


        int pack_send=0;
        for (int i=1; i<=Total_datapackets;i++) //normales DP
        {
            if((Total_datapackets-1)!=pack_send)
            {
            DP_Arr = new byte[Datasize+3]; 
            DP_buff = ByteBuffer.allocate(Datasize+3);

            DP_buff.clear();
            DP_buff.putShort(sessionnum); //sessionnr
            DP_buff.put((byte) ((i & 0xFF)%256));   //bzw. i%Fenstergröße+1
            DP_buff.put(fbuff,(pack_send*Datasize),Datasize);
            DP_buff.rewind();
            pack_send++;
            }
            else                           //letzes DP
            {
            //logger.log(Level.CONFIG, "Client-FT: " + "Datasize_last: "+ Datasize_last); 
            DP_Arr = new byte[Datasize_last+3]; 
            DP_buff = ByteBuffer.allocate(Datasize_last+3);

            DP_buff.clear();
            DP_buff.putShort(sessionnum); //sessionnr
            DP_buff.put((byte)((i & 0xFF)%256));   //bzw. i%Fenstergröße+1
            DP_buff.put(fbuff,(pack_send*Datasize),Datasize_last);
            DP_buff.rewind();
            pack_send=Total_datapackets;
            }
        /*2. Pakete schicken---------------------------------------*/ 

            DP_buff.get(DP_Arr);
            DP_buff.rewind();
            // logger.log(Level.CONFIG, "Client-FT: " + "sending DP_nr:"+pack_send+" "+"Sessionnr: " + sessionnum); 
            long startTime = System.currentTimeMillis();
            if(Total_datapackets!=pack_send)
            {myARQ.data_req(DP_Arr,Datasize+3,false);}
            else
            {myARQ.data_req(DP_Arr,Datasize_last+3,false);}
            long endTime = System.currentTimeMillis();
            long deltaTime = endTime - startTime;
            if(deltaTime<=1)
            {
                deltaTime=1;
            }
            Totaltime+=deltaTime;
            long datarate=1024/deltaTime; //byte/ms == Kbyte/s  
            // logger.log(Level.INFO, "deltatime: " +deltaTime+ " ms");
            logger.log(Level.INFO, "DP_nr: "+(pack_send%256)+" Datenrate: " +datarate*8+ " Kbit/s");

            
        }
        /*------------------------------Finales Paket------------------------------------------------------- */
        /* 1. Paket Bauen --------------------------------------- */
        

        int FP_size = 3+5+4; //sessnum+packNr+CRC32
        ByteBuffer FP_buff = ByteBuffer.allocate(FP_size);
        FP_buff.putShort(sessionnum); //2
        FP_buff.put((byte)((pack_send+1)%256));
        FP_buff.put((byte)'F').put((byte)'i').put((byte)'n').put((byte)'a').put((byte)'l'); //+5
        FP_buff.putInt(crc32sum);//4
        FP_buff.rewind();

        logger.log(Level.CONFIG, "Client-FT: Client_CRC32sum: "+crc32sum);

        byte[] FP_arr = new byte[FP_size];
        FP_buff.get(FP_arr);
        /* 2. Paket Senden --------------------------------------*/

        logger.log(Level.CONFIG, "Client-FT: sending  FP");
        // long startTime = System.currentTimeMillis();
        myARQ.data_req(FP_arr,FP_size,false);
        // long endTime = System.currentTimeMillis();
        //long deltaTime = endTime - startTime;
            logger.log(Level.INFO, "Totaltime: " +Totaltime+ " ms");
            logger.log(Level.INFO, "Filesize: " +fileSize*8+ " bit");
            long Avg_datarate=(fileSize*8)/(Totaltime);
            logger.log(Level.INFO, "Avg.Datenrate: " +Avg_datarate+ " Kbit/s");

        return true;
    }



    /*-----------------------------------------Server---------------------------------------------------------*/

    @Override
    public boolean file_init() throws IOException 
    {
        Path filePath=null;
        long fileSize=0;

        byte[] req;
        CRC32 server_crc32 = new CRC32();
        boolean reset_session=false;
       

        while(true)
        {
        /* Server  Online wartet auf paket kommt*/

        try 
        {
            if(reset_session==false)
            {
            req = myARQ.data_ind_req();            
            }
            else 
            {
                logger.log(Level.FINEST, "Ending Session"); 
                reset_session=false; 
                myARQ.closeConnection();
                break;
            }
            int req_length = req.length;
            ByteBuffer server_buff = ByteBuffer.wrap(req);
            if(req_length==3) //nur ACK zurückschicken
            {
                logger.log(Level.FINEST, "sending Ack again"); 
            }
            else    
            {
                short req_sessionnum=server_buff.getShort();
                byte req_packetnum=server_buff.get();


                if(req_length<=7) //letztes DP (wenn Daten <=  4Byte) oder FP
                {
                    

                    if(req_length==7)          //FP
                    {
                       // logger.log(Level.FINEST, "Server-FT: Final-Packet received");       
                        int rec_checksum=server_buff.getInt();
                        int fp_crc32_check=0;

                        byte[] Sfile=Files.readAllBytes(filePath);
                        server_crc32.reset();          
                        server_crc32.update(Sfile);
                        fp_crc32_check = (int) server_crc32.getValue(); 
                        logger.log(Level.FINEST, "Server-FT: Sfile_crc32: "+fp_crc32_check); 
                        logger.log(Level.FINEST, "Server-FT: rec_file_crc32: "+rec_checksum);
            
                        if(fp_crc32_check==rec_checksum)
                        {logger.log(Level.FINEST, "Server-FT: CrC32 der Dateien sind gleich");}
                        else
                        {logger.log(Level.FINEST, "Server-FT: CrC32 der Dateien sind nichtgleich");}

                        int FAck_size=2+1+1+4; 
                        ByteBuffer FAck_buff = ByteBuffer.allocate(FAck_size); 

                        FAck_buff.putShort(req_sessionnum);
                        FAck_buff.put((byte) (req_packetnum));  
                        FAck_buff.put((byte) 1);
                        FAck_buff.putInt(fp_crc32_check);   
                        FAck_buff.rewind();   

                        byte[] FAck_Arr= new byte[FAck_buff.remaining()];
                        FAck_buff.get(FAck_Arr);   

                        // logger.log(Level.FINEST, "Server-FT: sending FACK");
                        myARQ.data_req(FAck_Arr, FAck_size, true); //letztes ACK wird nicht 2 mal gesendet  
                        reset_session=true;
                    }
                    else              //letztes DP mit weniger als 5 byte datenanteil Server.getremaining()==byte
                    {
                        // logger.log(Level.FINEST, "Server-FT: Data-Packet received");

                        ByteBuffer req_buff= ByteBuffer.wrap(req);
                        req_buff.position(3);
                        writeByteBufferToFile(req_buff,filePath);
                    }
                }
                else if(req_length>=8) //SP oder  DP 
                {
                 byte[] signature=new byte[5];
                 server_buff.get(signature);
                 String req_marker= new  String(signature,StandardCharsets.UTF_8);
                 /*-------------------------------SP----------------------------*/
                 if(req_marker.equals("Start"))
                 {
                 logger.log(Level.FINEST, "Server-FT: " + "SP_received");
                 fileSize=server_buff.getLong();
                 short req_fNamelen=server_buff.getShort();
                 byte[] fName_arr=new byte[req_fNamelen];
                 server_buff.get(fName_arr);
                 String req_fName= new String (fName_arr,StandardCharsets.UTF_8);
                 int req_crc32=server_buff.getInt();
                 server_buff.rewind();

                 /*packet ausgelesen jetzt crc32 checken*/

                 int SP_check_size=5+8+2+(int) req_fNamelen;
                 ByteBuffer SP_check_buff= ByteBuffer.allocate(SP_check_size);
                 SP_check_buff.put(req_marker.getBytes());
                 SP_check_buff.putLong(fileSize);
                 SP_check_buff.putShort(req_fNamelen);   
                 SP_check_buff.put(req_fName.getBytes());   
                 SP_check_buff.rewind();

                 server_crc32.reset();
                 server_crc32.update(SP_check_buff);
                 int server_crc32_check_sp= (int) server_crc32.getValue();
                 //logger.log(Level.CONFIG, "server-FT: " + "owncrc32_SP: " + server_crc32_check_sp); 
                 //logger.log(Level.CONFIG, "server-FT: " + "reccrc32_SP: " + req_crc32); 

                  if(req_crc32==server_crc32_check_sp)
                  {
                    System.out.println("Server-FT crc32 ist gleich");
                    logger.log(Level.FINEST, "Server-FT: " + "trying to create");

                    String basefileName=req_fName;
                    filePath = Paths.get(FileCopy.dir);

                    int fileIndex = 1;
                    String [] fNamesplit = basefileName.split("\\.");

                    while (fNamesplit.length > 1 && Files.exists(filePath.resolve(req_fName))) 
                    {
                    req_fName = fNamesplit[0].replaceFirst("\\.*$", "") + fileIndex + "." + fNamesplit[1];
                    fileIndex++;
                    }
                    filePath = filePath.resolve(req_fName);
                    logger.log(Level.CONFIG, "Filepath:" + filePath);

                    // String fileContent = new String("Success");
                    // Files.write(filePath, fileContent.getBytes());
                  }             
                  else
                  {
                    System.out.println("crc32 ist nicht gleich session closed");
                    System.out.println("Try to send another Startpackage");
                    return false;
                  }

                 }
               /*--------------------------end SP---------------------------------------------- */
               /*--------------------------start DP-------------------------------------------- */
                else
                {
                //logger.log(Level.FINEST, "Server-FT: Data-Packet received");
                ByteBuffer req_buff= ByteBuffer.wrap(req);
                req_buff.position(3);  
                writeByteBufferToFile(req_buff,filePath);
                }               
            }   
         }
        }
        catch (TimeoutException e) 
        {
            throw new RuntimeException(e);
        }
     }         
     return true;
    }



     /*nicht mehr gebraucht  sehr nützlich zum testen

    private void WritetoFile(String FileName) throws IOException
    {
            byte[] fileData;
            try{
                fileData = myARQ.data_ind_req(0);

                    String basefileName=FileName;
                    Path filePath = Paths.get(FileCopy.dir);
            
            
            // Überprüfen, ob die Datei bereits existiert
            int fileIndex = 1;
            String [] fNamesplit = basefileName.split("\\.");
            //logger.log(Level.CONFIG, "basefileName:"+ basefileName + " fName: "+ FileName);
            //logger.log(Level.CONFIG, "Namesplit:0 " + fNamesplit[0] +"Namesplit:1 "+ fNamesplit[1]);


            while (fNamesplit.length > 1 && Files.exists(filePath.resolve(FileName))) 
            {
                FileName = fNamesplit[0].replaceFirst("\\.*$", "") + fileIndex + "." + fNamesplit[1];
                fileIndex++;
            }
            filePath = filePath.resolve(FileName);
            logger.log(Level.CONFIG, "Filepath:" + filePath);
            String fileContent = new String(fileData);
            Files.write(filePath, fileContent.getBytes());
            logger.log(Level.CONFIG, "Server-FT: File saved at " + filePath.toString());
             } catch (TimeoutException e)
            {
            throw new IOException("TimeoutException while receiving file data", e);
            }
    } */

    private static void writeByteBufferToFile(ByteBuffer buffer, Path filePath) {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            channel.write(buffer);
            //System.out.println("Daten erfolgreich an die Datei angehängt.");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Fehler beim Schreiben der Daten in die Datei.");
        }
    }

      /* Path testpath= Paths.get(FileCopy.dir);
         testpath = testpath.resolve("testpack2");
         writeByteBufferToFile(SP_buff,testpath); */
           
}