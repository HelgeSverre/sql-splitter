-- Microsoft SQL Server / T-SQL variant of production_shape. Square-bracket
-- identifiers, IDENTITY columns, N'unicode' literals, and GO batch separators.
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[users] (
  [id] INT IDENTITY(1,1) NOT NULL,
  [email] NVARCHAR(255) NOT NULL,
  [password] NVARCHAR(255) NOT NULL,
  [api_key] NVARCHAR(64) NULL,
  [token] NVARCHAR(64) NULL,
  [is_active] BIT NOT NULL,
  [balance] DECIMAL(12,2) NOT NULL,
  [metadata] NVARCHAR(MAX) NULL,
  [created_at] DATETIME NOT NULL,
  [updated_at] DATETIME NOT NULL,
  CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
)
GO
INSERT INTO [dbo].[users] ([id], [email], [password], [api_key], [token], [is_active], [balance], [metadata], [created_at], [updated_at]) VALUES
(1,N'alice@example.com',N'$2y$10$abcdefghijk',N'key_00000001',N'tok_a',1,10.50,N'{"tier":"gold"}','2024-01-01 10:00:00','2024-01-02 10:00:00'),
(2,N'bob@example.com',N'$2y$10$abcdefghijk',NULL,NULL,1,20.00,N'{"tier":"silver"}','2024-01-03 10:00:00','2024-01-04 10:00:00'),
(3,N'carol@example.com',N'$2y$10$abcdefghijk',N'key_00000003',NULL,0,0.00,N'{broken json','2024-02-01 10:00:00','2024-02-02 10:00:00'),
(4,N'dave@example.com',N'$2y$10$abcdefghijk',NULL,N'tok_d',1,5.25,N'{"tier":"gold"}','2024-03-01 10:00:00','2024-03-02 10:00:00'),
(5,N'erin@example.com',N'$2y$10$abcdefghijk',N'key_00000005',N'tok_e',1,100.00,N'{"tier":"gold"}','2024-04-01 10:00:00','2024-04-02 10:00:00'),
(6,N'frank@example.com',N'$2y$10$abcdefghijk',N'key_00000006',N'tok_f',0,1000.00,N'{"tier":"silver"}','2024-05-01 10:00:00','2024-05-02 10:00:00')
GO
CREATE TABLE [dbo].[orders] (
  [id] INT IDENTITY(1,1) NOT NULL,
  [user_id] INT NOT NULL,
  [total] DECIMAL(12,2) NOT NULL,
  [status] NVARCHAR(32) NOT NULL,
  [created_at] DATETIME NOT NULL,
  [updated_at] DATETIME NOT NULL,
  CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id]),
  CONSTRAINT [FK_orders_users] FOREIGN KEY ([user_id]) REFERENCES [dbo].[users] ([id])
)
GO
INSERT INTO [dbo].[orders] ([id], [user_id], [total], [status], [created_at], [updated_at]) VALUES
(1,1,10.50,N'paid','2024-01-05 10:00:00','2024-01-06 10:00:00'),
(2,1,20.00,N'paid','2024-01-07 10:00:00','2024-01-08 10:00:00'),
(3,2,5.25,N'pending','2024-02-05 10:00:00','2024-02-06 10:00:00'),
(4,3,100.00,N'paid','2024-04-05 10:00:00','2024-04-06 10:00:00'),
(5,5,1000.00,N'paid','2024-05-05 10:00:00','2024-05-06 10:00:00')
GO
