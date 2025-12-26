SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET NOCOUNT ON
GO

-- Table with IDENTITY and various MSSQL types
CREATE TABLE [dbo].[products] (
    [id] BIGINT IDENTITY(100,10) NOT NULL,
    [sku] NVARCHAR(50) NOT NULL,
    [name] NVARCHAR(255) NOT NULL,
    [description] NVARCHAR(MAX),
    [price] MONEY NOT NULL,
    [cost] SMALLMONEY,
    [quantity] INT DEFAULT 0,
    [is_active] BIT DEFAULT 1,
    [weight] FLOAT,
    [dimensions] REAL,
    [created_at] DATETIME2(7) DEFAULT GETDATE(),
    [updated_at] DATETIME2 DEFAULT SYSDATETIME(),
    [launch_date] DATE,
    [sale_time] TIME(3),
    [guid] UNIQUEIDENTIFIER DEFAULT NEWID(),
    [metadata] NVARCHAR(MAX),
    [binary_data] VARBINARY(MAX),
    [image] IMAGE,
    [small_binary] VARBINARY(255),
    [row_version] ROWVERSION,
    CONSTRAINT [PK_products] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY]
GO

-- Table with composite primary key
CREATE TABLE [dbo].[order_items] (
    [order_id] INT NOT NULL,
    [product_id] BIGINT NOT NULL,
    [quantity] INT NOT NULL,
    [unit_price] DECIMAL(10,2) NOT NULL,
    [discount] DECIMAL(5,2) DEFAULT 0.00,
    CONSTRAINT [PK_order_items] PRIMARY KEY CLUSTERED ([order_id], [product_id]),
    CONSTRAINT [FK_order_items_orders] FOREIGN KEY ([order_id]) REFERENCES [dbo].[orders]([id]),
    CONSTRAINT [FK_order_items_products] FOREIGN KEY ([product_id]) REFERENCES [dbo].[products]([id])
) ON [PRIMARY]
GO

-- Table with NONCLUSTERED index and INCLUDE
CREATE NONCLUSTERED INDEX [IX_products_sku] ON [dbo].[products] ([sku])
GO

CREATE NONCLUSTERED INDEX [IX_products_name_price] ON [dbo].[products] ([name], [price])
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_products_guid] ON [dbo].[products] ([guid])
GO

-- Insert with unicode strings
INSERT INTO [dbo].[products] ([sku], [name], [description], [price], [is_active])
VALUES (N'SKU-001', N'日本語製品', N'これは日本語の説明です', 99.99, 1)
GO

INSERT INTO [dbo].[products] ([sku], [name], [description], [price], [is_active])
VALUES (N'SKU-002', N'Ελληνικό προϊόν', N'Αυτή είναι ελληνική περιγραφή', 149.50, 1)
GO

INSERT INTO [dbo].[products] ([sku], [name], [description], [price], [is_active])
VALUES (N'SKU-003', N'Product with ''quotes''', N'Description with N''escaped quotes''', 199.99, 1)
GO

-- Insert with hex binary data
INSERT INTO [dbo].[products] ([sku], [name], [price], [binary_data], [is_active])
VALUES (N'SKU-004', N'Binary Product', 299.99, 0x48454C4C4F574F524C44, 1)
GO

-- Insert with NULL values
INSERT INTO [dbo].[products] ([sku], [name], [price], [description], [cost], [weight], [is_active])
VALUES (N'SKU-005', N'Minimal Product', 9.99, NULL, NULL, NULL, 0)
GO

-- Table with datetime variations
CREATE TABLE [dbo].[events] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [name] NVARCHAR(100) NOT NULL,
    [event_date] DATE NOT NULL,
    [event_time] TIME NOT NULL,
    [created] DATETIME NOT NULL,
    [created2] DATETIME2 NOT NULL,
    [created_offset] DATETIMEOFFSET NOT NULL,
    [small_date] SMALLDATETIME,
    CONSTRAINT [PK_events] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY]
GO

INSERT INTO [dbo].[events] ([name], [event_date], [event_time], [created], [created2], [created_offset])
VALUES (N'Test Event', '2025-01-15', '14:30:00', '2025-01-15 14:30:00', '2025-01-15 14:30:00.1234567', '2025-01-15 14:30:00.1234567 +05:30')
GO
