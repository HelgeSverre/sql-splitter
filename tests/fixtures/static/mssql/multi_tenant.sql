SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Multi-tenant schema for shard testing
CREATE TABLE [dbo].[tenants] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [name] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_tenants] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [tenant_id] INT NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [name] NVARCHAR(100),
    CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id]),
    CONSTRAINT [FK_users_tenants] FOREIGN KEY ([tenant_id]) REFERENCES [dbo].[tenants]([id])
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[orders] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [tenant_id] INT NOT NULL,
    [user_id] INT NOT NULL,
    [total] DECIMAL(10,2),
    [status] NVARCHAR(50),
    CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id]),
    CONSTRAINT [FK_orders_tenants] FOREIGN KEY ([tenant_id]) REFERENCES [dbo].[tenants]([id]),
    CONSTRAINT [FK_orders_users] FOREIGN KEY ([user_id]) REFERENCES [dbo].[users]([id])
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[countries] (
    [code] CHAR(2) NOT NULL,
    [name] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_countries] PRIMARY KEY CLUSTERED ([code])
) ON [PRIMARY]
GO

INSERT INTO [dbo].[tenants] ([id], [name]) VALUES (1, N'Acme Corp')
GO
INSERT INTO [dbo].[tenants] ([id], [name]) VALUES (2, N'Globex Inc')
GO

INSERT INTO [dbo].[users] ([id], [tenant_id], [email], [name]) VALUES (1, 1, N'alice@acme.com', N'Alice')
GO
INSERT INTO [dbo].[users] ([id], [tenant_id], [email], [name]) VALUES (2, 1, N'bob@acme.com', N'Bob')
GO
INSERT INTO [dbo].[users] ([id], [tenant_id], [email], [name]) VALUES (3, 2, N'carol@globex.com', N'Carol')
GO

INSERT INTO [dbo].[orders] ([id], [tenant_id], [user_id], [total], [status]) VALUES (1, 1, 1, 99.99, N'completed')
GO
INSERT INTO [dbo].[orders] ([id], [tenant_id], [user_id], [total], [status]) VALUES (2, 1, 2, 49.50, N'pending')
GO
INSERT INTO [dbo].[orders] ([id], [tenant_id], [user_id], [total], [status]) VALUES (3, 2, 3, 199.99, N'completed')
GO

INSERT INTO [dbo].[countries] ([code], [name]) VALUES ('US', N'United States')
GO
INSERT INTO [dbo].[countries] ([code], [name]) VALUES ('UK', N'United Kingdom')
GO
