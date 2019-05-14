/*Enable Broker on the DataBase*/
--ALTER DATABASE Database SET ENABLE_BROKER with rollback immediate

/*Create massages types*/
CREATE MESSAGE TYPE
[Request]
VALIDATION = WELL_FORMED_XML


CREATE MESSAGE TYPE
[Reply]
VALIDATION = WELL_FORMED_XML

/*Create contract*/

CREATE CONTRACT [TestContract] 
(
	[Request] SENT BY INITIATOR	,
	[Reply] SENT BY TARGET	
)

/*Create Queues*/

CREATE QUEUE InitialQueue   WITH STATUS = ON ;  
CREATE QUEUE TargetQueue  WITH STATUS = ON ; 

/*Create Service*/
CREATE SERVICE InitialService ON QUEUE InitialQueue;
CREATE SERVICE TargetService ON QUEUE TargetQueue;

ALTER SERVICE TargetService (ADD CONTRACT TestContract)
ALTER SERVICE InitialService (ADD CONTRACT TestContract)

--ALTER QUEUE InitialQueue WITH STATUS = ON ;  
--ALTER QUEUE TargetQueue WITH STATUS = ON ;  

/*This is where the magic comes in*/
/*Send initail message to target*/

DECLARE @Handler UNIQUEIDENTIFIER;
DECLARE @RequestMessage NVARCHAR(1000);

BEGIN TRAN
BEGIN DIALOG @Handler
FROM SERVICE [InitialService] 
TO SERVICE	'TargetService'
ON CONTRACT [TestContract]
WITH ENCRYPTION =off;

SET @RequestMessage ='<Request>Do something</Request>';

SEND ON CONVERSATION @Handler
MESSAGE TYPE [Request] (@RequestMessage);


SELECT iq.*, convert(nvarchar(2000), message_body) FROM InitialQueue iq
SELECT tq.*, convert(nvarchar(2000), message_body) from TargetQueue tq 

commit 
GO


/*Target Queue Processor*/


BEGIN TRAN
DECLARE @Handler UNIQUEIDENTIFIER;
DECLARE @ReplyMessage nvarchar(1000);
DECLARE @ReplyMessageName  Sysname;

RECEIVE TOP(1) @Handler = Conversation_Handle, @ReplyMessage = Message_Body, @ReplyMessageName = Message_Type_Name   FROM TargetQueue


  if (@ReplyMessageName = 'Request')
  BEGIN
  /*Doing Something*/


  SET @ReplyMessage = '<Reply>Done</Reply>';
  SEND ON CONVERSATION @Handler MESSAGE TYPE [Reply] (@ReplyMessage);
  END CONVERSATION @Handler
  END

COMMIT
go

/*Initial Queue Processor*/

BEGIN TRAN
DECLARE @Handler UNIQUEIDENTIFIER;
DECLARE @ReplyMessage nvarchar(1000);
DECLARE @ReplyMessageName  Sysname;

RECEIVE TOP(1) @Handler = Conversation_Handle, @ReplyMessage = Message_Body, @ReplyMessageName = Message_Type_Name   FROM InitialQueue


  if (@ReplyMessageName = 'Reply')
  BEGIN
  /*Register reply*/
  PRINT @ReplyMessage

  END CONVERSATION @Handler
  END

commit
go





















