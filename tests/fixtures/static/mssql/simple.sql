SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [name] NVARCHAR(100),
    [created_at] DATETIME2(7) DEFAULT GETDATE(),
    CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[orders] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [user_id] INT NOT NULL,
    [total] DECIMAL(10,2),
    [status] NVARCHAR(50) DEFAULT N'pending',
    CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id]),
    CONSTRAINT [FK_orders_users] FOREIGN KEY ([user_id]) REFERENCES [dbo].[users]([id])
) ON [PRIMARY]
GO

INSERT INTO [dbo].[users] ([email], [name]) VALUES (N'alice@example.com', N'Alice')
GO
INSERT INTO [dbo].[users] ([email], [name]) VALUES (N'bob@example.com', N'Bob')
GO

INSERT INTO [dbo].[orders] ([user_id], [total], [status]) VALUES (1, 99.99, N'completed')
GO
INSERT INTO [dbo].[orders] ([user_id], [total], [status]) VALUES (1, 49.50, N'pending')
GO
INSERT INTO [dbo].[orders] ([user_id], [total], [status]) VALUES (2, 199.99, N'completed')
GO

CREATE NONCLUSTERED INDEX [IX_users_email] ON [dbo].[users] ([email])
GO

CREATE NONCLUSTERED INDEX [IX_orders_user_id] ON [dbo].[orders] ([user_id])
GO
